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

package io.aiven.connect.jdbc.source;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialect;

import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceTaskLifecycleTest extends JdbcSourceTaskTestBase {

    @Mock
    private SourceConnectionProvider mockSourceConnectionProvider;

    @Mock
    private Connection conn;

    @Test(expected = ConnectException.class)
    public void testMissingParentConfig() {
        final Map<String, String> props = singleTableConfig();
        props.remove(JdbcConfig.CONNECTION_URL_CONFIG);
        task.start(props);
    }

    @Test(expected = ConnectException.class)
    public void testMissingTables() {
        final Map<String, String> props = singleTableConfig();
        props.remove(JdbcSourceTaskConfig.TABLES_CONFIG);
        task.start(props);
    }

    @Test
    public void testStartStop() throws Exception {
        // Minimal start/stop functionality
        PowerMock.expectNew(
            SourceConnectionProvider.class,
            EasyMock.anyObject(DatabaseDialect.class),
            EasyMock.eq(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DEFAULT),
            EasyMock.eq(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DEFAULT))
            .andReturn(mockSourceConnectionProvider);

        // Should request a connection, then should close it on stop()
        EasyMock.expect(mockSourceConnectionProvider.getConnection()).andReturn(db.getConnection());

        PowerMock.expectLastCall();

        PowerMock.replayAll();

        task.start(singleTableConfig());
        task.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPollInterval() throws Exception {
        // Here we just want to verify behavior of the poll method, not any loading of data, so we
        // specifically want an empty
        db.createTable(SINGLE_TABLE_NAME, "id", "INT");
        // Need data or poll() never returns
        db.insert(SINGLE_TABLE_NAME, "id", 1);

        final long startTime = time.milliseconds();
        task.start(singleTableConfig());

        // First poll should happen immediately
        task.poll();
        assertEquals(startTime, time.milliseconds());

        // Subsequent polls have to wait for timeout
        task.poll();
        assertEquals(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
            time.milliseconds());
        task.poll();
        assertEquals(startTime + 2 * JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
            time.milliseconds());

        task.stop();
    }


    @Test
    public void testSingleUpdateMultiplePoll() throws Exception {
        // Test that splitting up a table update query across multiple poll() calls works

        db.createTable(SINGLE_TABLE_NAME, "id", "INT");

        final Map<String, String> taskConfig = singleTableConfig();
        taskConfig.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, "1");
        final long startTime = time.milliseconds();
        task.start(taskConfig);

        // Two entries should get split across three poll() calls with no delay
        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 2);

        List<SourceRecord> records = task.poll();
        assertEquals(startTime, time.milliseconds());
        assertEquals(1, records.size());
        records = task.poll();
        assertEquals(startTime, time.milliseconds());
        assertEquals(1, records.size());

        // Subsequent poll should wait for next timeout
        task.poll();
        assertEquals(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
            time.milliseconds());

    }

    @Test
    public void testMultipleTables() throws Exception {
        db.createTable(SINGLE_TABLE_NAME, "id", "INT");
        db.createTable(SECOND_TABLE_NAME, "id", "INT");

        final long startTime = time.milliseconds();
        task.start(twoTableConfig());

        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SECOND_TABLE_NAME, "id", 2);

        // Both tables should be polled immediately, in order
        List<SourceRecord> records = task.poll();
        assertEquals(startTime, time.milliseconds());
        assertEquals(1, records.size());
        assertEquals(SINGLE_TABLE_PARTITION, records.get(0).sourcePartition());
        records = task.poll();
        assertEquals(startTime, time.milliseconds());
        assertEquals(1, records.size());
        assertEquals(SECOND_TABLE_PARTITION, records.get(0).sourcePartition());

        // Subsequent poll should wait for next timeout
        records = task.poll();
        assertEquals(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
            time.milliseconds());
        validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
        records = task.poll();
        assertEquals(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
            time.milliseconds());
        validatePollResultTable(records, 1, SECOND_TABLE_NAME);

    }

    @Test
    public void testMultipleTablesMultiplePolls() throws Exception {
        // Check correct handling of multiple tables when the tables require multiple poll() calls to
        // return one query's data

        db.createTable(SINGLE_TABLE_NAME, "id", "INT");
        db.createTable(SECOND_TABLE_NAME, "id", "INT");

        final Map<String, String> taskConfig = twoTableConfig();
        taskConfig.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, "1");
        final long startTime = time.milliseconds();
        task.start(taskConfig);

        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 2);
        db.insert(SECOND_TABLE_NAME, "id", 3);
        db.insert(SECOND_TABLE_NAME, "id", 4);

        // Both tables should be polled immediately, in order
        for (int i = 0; i < 2; i++) {
            final List<SourceRecord> records = task.poll();
            assertEquals(startTime, time.milliseconds());
            validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
        }
        for (int i = 0; i < 2; i++) {
            final List<SourceRecord> records = task.poll();
            assertEquals(startTime, time.milliseconds());
            validatePollResultTable(records, 1, SECOND_TABLE_NAME);
        }

        // Subsequent poll should wait for next timeout
        for (int i = 0; i < 2; i++) {
            final List<SourceRecord> records = task.poll();
            assertEquals(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
                time.milliseconds());
            validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
        }
        for (int i = 0; i < 2; i++) {
            final List<SourceRecord> records = task.poll();
            assertEquals(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
                time.milliseconds());
            validatePollResultTable(records, 1, SECOND_TABLE_NAME);
        }
    }

    private static void validatePollResultTable(final List<SourceRecord> records,
                                                final int expected, final String table) {
        assertEquals(expected, records.size());
        for (final SourceRecord record : records) {
            assertEquals(table, record.sourcePartition().get(JdbcSourceConnectorConstants.TABLE_NAME_KEY));
        }
    }
}
