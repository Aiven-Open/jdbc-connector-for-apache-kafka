/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.source.EmbeddedDerby;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.aiven.connect.jdbc.source.JdbcSourceTask;
import io.aiven.connect.jdbc.source.JdbcSourceTaskConfig;
import io.aiven.connect.jdbc.util.CachedConnectionProvider;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.TableId;

import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceConnector.class, DatabaseDialect.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceConnectorTest {

    private JdbcSourceConnector connector;
    private EmbeddedDerby db;
    private Map<String, String> connProps;

    @Mock
    private DatabaseDialect dialect;

    @Before
    public void setup() {
        connector = new JdbcSourceConnector();
        db = new EmbeddedDerby();
        connProps = new HashMap<>();
        connProps.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    }

    @After
    public void tearDown() throws Exception {
        db.close();
        db.dropDatabase();
    }

    @Test
    public void testTaskClass() {
        assertEquals(JdbcSourceTask.class, connector.taskClass());
    }

    @Test(expected = ConnectException.class)
    public void testMissingUrlConfig() throws Exception {
        final HashMap<String, String> connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connector.start(connProps);
    }

    @Test(expected = ConnectException.class)
    public void testMissingModeConfig() throws Exception {
        final HashMap<String, String> connProps = new HashMap<>();
        connProps.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        connector.start(Collections.<String, String>emptyMap());
    }

    @Test(expected = ConnectException.class)
    public void testStartConnectionFailure() throws Exception {
        // Invalid URL
        connector.start(Collections.singletonMap(JdbcConfig.CONNECTION_URL_CONFIG, "jdbc:foo"));
    }

    @Test
    public void testStartStop() throws Exception {
        final CachedConnectionProvider mockCachedConnectionProvider =
            PowerMock.createMock(CachedConnectionProvider.class);
        PowerMock.expectNew(
            CachedConnectionProvider.class,
            EasyMock.anyObject(DatabaseDialect.class),
            EasyMock.eq(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DEFAULT),
            EasyMock.eq(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DEFAULT))
            .andReturn(mockCachedConnectionProvider);

        // Should request a connection, then should close it on stop(). The background thread may also
        // request connections any time it performs updates.
        final Connection conn = PowerMock.createMock(Connection.class);
        EasyMock.expect(mockCachedConnectionProvider.getConnection()).andReturn(conn).anyTimes();

        // Since we're just testing start/stop, we don't worry about the value here but need to stub
        // something since the background thread will be started and try to lookup metadata.
        EasyMock.expect(conn.getMetaData()).andStubThrow(new SQLException());
        // Close will be invoked both for the SQLExeption and when the connector is stopped
        mockCachedConnectionProvider.close();
        PowerMock.expectLastCall().atLeastOnce();

        PowerMock.replayAll();

        connector.start(connProps);
        connector.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPartitioningOneTable() throws Exception {
        // Tests simplest case where we have exactly 1 table and also ensures we return fewer tasks
        // if there aren't enough tables for the max # of tasks
        db.createTable("test", "id", "INT NOT NULL");
        connector.start(connProps);
        final List<Map<String, String>> configs = connector.taskConfigs(10);
        assertEquals(1, configs.size());
        assertTaskConfigsHaveParentConfigs(configs);
        assertEquals(tables("test"), configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
        connector.stop();
    }

    @Test
    public void testPartitioningManyTables() throws Exception {
        // Tests distributing tables across multiple tasks, in this case unevenly
        db.createTable("test1", "id", "INT NOT NULL");
        db.createTable("test2", "id", "INT NOT NULL");
        db.createTable("test3", "id", "INT NOT NULL");
        db.createTable("test4", "id", "INT NOT NULL");
        connector.start(connProps);
        final List<Map<String, String>> configs = connector.taskConfigs(3);
        assertEquals(3, configs.size());
        assertTaskConfigsHaveParentConfigs(configs);

        assertEquals(tables("test1", "test2"), configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
        assertEquals(tables("test3"), configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertNull(configs.get(1).get(JdbcSourceTaskConfig.QUERY_CONFIG));
        assertEquals(tables("test4"), configs.get(2).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertNull(configs.get(2).get(JdbcSourceTaskConfig.QUERY_CONFIG));

        connector.stop();
    }

    @Test
    public void testPartitioningUnqualifiedTables() throws Exception {
        connProps.put(JdbcSourceConnectorConfig.TABLE_NAMES_QUALIFY_CONFIG, "false");
        // Tests distributing tables across multiple tasks, in this case unevenly
        db.createTable("test1", "id", "INT NOT NULL");
        db.createTable("test2", "id", "INT NOT NULL");
        db.createTable("test3", "id", "INT NOT NULL");
        db.createTable("test4", "id", "INT NOT NULL");
        connector.start(connProps);
        final List<Map<String, String>> configs = connector.taskConfigs(3);
        assertEquals(3, configs.size());
        assertTaskConfigsHaveParentConfigs(configs);

        assertEquals(unqualifiedTables("test1", "test2"), configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
        assertEquals(unqualifiedTables("test3"), configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertNull(configs.get(1).get(JdbcSourceTaskConfig.QUERY_CONFIG));
        assertEquals(unqualifiedTables("test4"), configs.get(2).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertNull(configs.get(2).get(JdbcSourceTaskConfig.QUERY_CONFIG));

        connector.stop();
    }

    @Test
    public void testPartitioningQuery() throws Exception {
        // Tests "partitioning" when config specifies running a custom query
        db.createTable("test1", "id", "INT NOT NULL");
        db.createTable("test2", "id", "INT NOT NULL");
        final String sampleQuery = "SELECT foo, bar FROM sample_table";
        connProps.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sampleQuery);
        connector.start(connProps);
        final List<Map<String, String>> configs = connector.taskConfigs(3);
        assertEquals(1, configs.size());
        assertTaskConfigsHaveParentConfigs(configs);

        assertEquals("", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
        assertEquals(sampleQuery, configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));

        connector.stop();
    }

    @Test
    public void testConflictingQueryTableSettings() {
        final String sampleQuery = "SELECT foo, bar FROM sample_table";
        connProps.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sampleQuery);
        connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "foo,bar");
        assertThrows(ConnectException.class, () -> connector.start(connProps));

        connector = new JdbcSourceConnector();
        connProps.remove(JdbcSourceConnectorConfig.QUERY_CONFIG);
        connProps.put(JdbcSourceConnectorConfig.TABLE_NAMES_QUALIFY_CONFIG, "false");
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
        assertThrows(ConnectException.class, () -> connector.start(connProps));

        connector = new JdbcSourceConnector();
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING);
        assertThrows(ConnectException.class, () -> connector.start(connProps));
    }

    private void assertTaskConfigsHaveParentConfigs(final List<Map<String, String>> configs) {
        for (final Map<String, String> config : configs) {
            assertEquals(this.db.getUrl(),
                config.get(JdbcConfig.CONNECTION_URL_CONFIG));
        }
    }

    private String tables(final String... names) {
        return tables(true, names);
    }

    private String unqualifiedTables(final String... names) {
        return tables(false, names);
    }

    private String tables(final boolean qualified, final String... names) {
        final String schema = qualified ? "APP" : null;
        final List<TableId> tableIds = new ArrayList<>();
        for (final String name : names) {
            tableIds.add(new TableId(null, schema, name));
        }
        final ExpressionBuilder builder = ExpressionBuilder.create();
        builder.appendList().delimitedBy(",").of(tableIds);
        return builder.toString();
    }
}
