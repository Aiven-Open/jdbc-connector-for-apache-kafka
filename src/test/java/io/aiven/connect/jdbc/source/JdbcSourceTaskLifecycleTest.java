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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
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
    public void testStartStop() {
        try (final var mock = mockConstruction(SourceConnectionProvider.class, (m, c) -> {
            when(m.getConnection()).thenReturn(db.getConnection());
        })) {
            task.start(singleTableConfig());
            task.stop();
            verify(mock.constructed().get(0)).getConnection();
        }
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
        assertThat(time.milliseconds()).isEqualTo(startTime);

        // Subsequent polls have to wait for timeout
        task.poll();
        assertThat(time.milliseconds()).isEqualTo(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT);
        task.poll();
        assertThat(time.milliseconds()).isEqualTo(startTime + 2 * JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT);

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
        assertThat(time.milliseconds()).isEqualTo(startTime);
        assertThat(records).hasSize(1);
        records = task.poll();
        assertThat(time.milliseconds()).isEqualTo(startTime);
        assertThat(records).hasSize(1);

        // Subsequent poll should wait for next timeout
        task.poll();
        assertThat(time.milliseconds()).isEqualTo(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT);

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
        assertThat(time.milliseconds()).isEqualTo(startTime);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).sourcePartition()).isEqualTo(SINGLE_TABLE_PARTITION);
        records = task.poll();
        assertThat(time.milliseconds()).isEqualTo(startTime);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).sourcePartition()).isEqualTo(SECOND_TABLE_PARTITION);

        // Subsequent poll should wait for next timeout
        records = task.poll();
        assertThat(time.milliseconds()).isEqualTo(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT);
        validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
        records = task.poll();
        assertThat(time.milliseconds()).isEqualTo(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT);
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
            assertThat(time.milliseconds()).isEqualTo(startTime);
            validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
        }
        for (int i = 0; i < 2; i++) {
            final List<SourceRecord> records = task.poll();
            assertThat(time.milliseconds()).isEqualTo(startTime);
            validatePollResultTable(records, 1, SECOND_TABLE_NAME);
        }

        // Subsequent poll should wait for next timeout
        for (int i = 0; i < 2; i++) {
            final List<SourceRecord> records = task.poll();
            assertThat(time.milliseconds()).isEqualTo(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT);
            validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
        }
        for (int i = 0; i < 2; i++) {
            final List<SourceRecord> records = task.poll();
            assertThat(time.milliseconds()).isEqualTo(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT);
            validatePollResultTable(records, 1, SECOND_TABLE_NAME);
        }
    }

    private static void validatePollResultTable(final List<SourceRecord> records,
                                                final int expected, final String table) {
        assertThat(records).hasSize(expected);
        for (final SourceRecord record : records) {
            assertThat(record.sourcePartition().get(JdbcSourceConnectorConstants.TABLE_NAME_KEY)).isEqualTo(table);
        }
    }
}
