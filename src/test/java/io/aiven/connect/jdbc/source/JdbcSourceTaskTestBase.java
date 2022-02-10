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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.util.TableId;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.powermock.api.easymock.annotation.Mock;

import static io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;

public class JdbcSourceTaskTestBase {

    protected static final String SINGLE_TABLE_NAME = "test";
    protected static final TableId SINGLE_TABLE_ID = new TableId(null, null, SINGLE_TABLE_NAME);
    protected static final Map<String, String> SINGLE_TABLE_PARTITION =
        OffsetProtocols.sourcePartitionForProtocolV0(SINGLE_TABLE_ID);
    protected static final Map<String, String> SINGLE_TABLE_PARTITION_WITH_VERSION =
        OffsetProtocols.sourcePartitionForProtocolV1(SINGLE_TABLE_ID);

    protected static final EmbeddedDerby.TableName SINGLE_TABLE
        = new EmbeddedDerby.TableName(SINGLE_TABLE_NAME);

    protected static final String SECOND_TABLE_NAME = "test2";
    protected static final Map<String, Object> SECOND_TABLE_PARTITION = new HashMap<>();

    static {
        SECOND_TABLE_PARTITION.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, SECOND_TABLE_NAME);
    }

    protected static final EmbeddedDerby.TableName SECOND_TABLE
        = new EmbeddedDerby.TableName(SECOND_TABLE_NAME);

    protected static final String JOIN_TABLE_NAME = "users";
    protected static final Map<String, Object> JOIN_QUERY_PARTITION = new HashMap<>();

    static {
        JOIN_QUERY_PARTITION.put(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
            JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
    }

    protected static final EmbeddedDerby.TableName JOIN_TABLE
        = new EmbeddedDerby.TableName(JOIN_TABLE_NAME);

    protected static final String TOPIC_PREFIX = "test-";

    protected Time time;
    @Mock
    protected SourceTaskContext taskContext;
    protected JdbcSourceTask task;
    protected EmbeddedDerby db;
    @Mock
    private OffsetStorageReader reader;

    @Before
    public void setup() throws Exception {
        time = new MockTime();
        task = new JdbcSourceTask(time);
        db = new EmbeddedDerby();
    }

    @After
    public void tearDown() throws Exception {
        db.close();
        db.dropDatabase();
    }

    protected Map<String, String> singleTableConfig() {
        return singleTableConfig(false);
    }

    protected Map<String, String> singleTableConfig(final boolean completeMapping) {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME);
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
        if (completeMapping) {
            props.put(JdbcSourceTaskConfig.NUMERIC_MAPPING_CONFIG, NumericMapping.BEST_FIT.toString());
        } else {
            props.put(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG, "true");
        }
        return props;
    }

    protected Map<String, String> twoTableConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME + "," + SECOND_TABLE_NAME);
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
        return props;
    }

    protected <T> void expectInitialize(final Collection<Map<String, T>> partitions,
                                        final Map<Map<String, T>, Map<String, Object>> offsets) {
        EasyMock.expect(taskContext.offsetStorageReader()).andReturn(reader);
        EasyMock.expect(reader.offsets(EasyMock.eq(partitions))).andReturn(offsets);
    }

    protected <T> void expectInitializeNoOffsets(final Collection<Map<String, T>> partitions) {
        final Map<Map<String, T>, Map<String, Object>> offsets = new HashMap<>();

        for (final Map<String, T> partition : partitions) {
            offsets.put(partition, null);
        }
        expectInitialize(partitions, offsets);
    }

    protected void initializeTask() {
        task.initialize(taskContext);
    }

}
