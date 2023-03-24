/*
 * Copyright 2022 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

package io.aiven.kafka.connect.jdbc.postgres;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import io.aiven.connect.jdbc.JdbcSourceConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnqualifiedTableNamesIntegrationTest extends AbstractPostgresIT {
    private static final String CONNECTOR_NAME = "test-source-connector";

    private static final String TABLE = "dup";
    private static final String PREFERRED_SCHEMA = "preferred";
    private static final String OTHER_SCHEMA = "other";

    private static final String CREATE_PREFERRED_TABLE =
            "create table " + PREFERRED_SCHEMA + "." + TABLE + "\n"
                    + "(\n"
                    + "    id int          generated always as identity primary key,\n"
                    + "    name  text      not null,\n"
                    + "    value text      not null,\n"
                    + "    date  timestamp not null default current_timestamp\n"
                    + ")";
    private static final String POPULATE_PREFERRED_TABLE =
            "insert into " + PREFERRED_SCHEMA + "." + TABLE + " (name, value) values\n"
                    + "('clef', 'bass')";
    private static final String CREATE_OTHER_TABLE =
            "create table " + OTHER_SCHEMA + "." + TABLE + "\n"
                    + "(\n"
                    + "    name  text      not null"
                    + ")";
    private static final String POPULATE_OTHER_TABLE =
            "insert into " + OTHER_SCHEMA + "." + TABLE + " (name) values\n"
                    + "('Rapu')"; // 🦀

    @Test
    public void testSingleTable() throws Exception {
        createTopic(TABLE, 1);
        consumer.assign(Collections.singleton(new TopicPartition(TABLE, 0)));
        // Make sure that the topic starts empty
        assertEmptyPoll(Duration.ofSeconds(1));

        executeUpdate(createSchema(PREFERRED_SCHEMA));
        executeUpdate(setSearchPath(postgreSqlContainer.getDatabaseName()));
        executeUpdate(CREATE_PREFERRED_TABLE);
        executeUpdate(POPULATE_PREFERRED_TABLE);
        connectRunner.createConnector(basicTimestampModeSourceConnectorConfig());

        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                .until(this::assertSingleNewRecordProduced);
    }

    @Test
    public void testMultipleTablesTimestampMode() throws Exception {
        testMultipleTables(basicTimestampModeSourceConnectorConfig());
    }

    @Test
    public void testMultipleTablesIncrementingMode() throws Exception {
        final Map<String, String> connectorConfig = basicSourceConnectorConfig();
        connectorConfig.put("mode", "incrementing");
        connectorConfig.put("incrementing.column.name", "id");
        testMultipleTables(connectorConfig);
    }

    @Test
    public void testMultipleTablesTimestampIncrementingMode() throws Exception {
        final Map<String, String> connectorConfig = basicSourceConnectorConfig();
        connectorConfig.put("mode", "timestamp+incrementing");
        connectorConfig.put("incrementing.column.name", "id");
        connectorConfig.put("timestamp.column.name", "date");
        testMultipleTables(connectorConfig);
    }

    private void testMultipleTables(final Map<String, String> connectorConfig) throws Exception {
        createTopic(TABLE, 1);
        consumer.assign(Collections.singleton(new TopicPartition(TABLE, 0)));
        // Make sure that the topic starts empty
        assertEmptyPoll(Duration.ofSeconds(1));

        executeUpdate(createSchema(PREFERRED_SCHEMA));
        executeUpdate(createSchema(OTHER_SCHEMA));
        executeUpdate(setSearchPath(postgreSqlContainer.getDatabaseName()));
        executeUpdate(CREATE_PREFERRED_TABLE);
        executeUpdate(POPULATE_PREFERRED_TABLE);
        executeUpdate(CREATE_OTHER_TABLE);
        executeUpdate(POPULATE_OTHER_TABLE);
        connectRunner.createConnector(connectorConfig);

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100))
                .until(this::assertSingleNewRecordProduced);

        executeUpdate(POPULATE_OTHER_TABLE);

        // Make sure that, even after adding another row to the other table, the connector
        // doesn't publish any new records
        assertEmptyPoll(Duration.ofSeconds(5));

        // Add one more row to the preferred table, and verify that the connector
        // is able to read it
        executeUpdate(POPULATE_PREFERRED_TABLE);
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100))
                .until(this::assertSingleNewRecordProduced);

        // Restart the connector, to ensure that offsets are tracked correctly
        connectRunner.restartTask(CONNECTOR_NAME, 0);

        // Add one more row to the preferred table, and verify that the connector
        // is able to read it
        executeUpdate(POPULATE_PREFERRED_TABLE);
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100))
                .until(this::assertSingleNewRecordProduced);

        // Make sure that the connector doesn't publish any more records
        assertEmptyPoll(Duration.ofSeconds(5));
    }

    private void assertEmptyPoll(final Duration duration) {
        final ConsumerRecords<?, ?> records = consumer.poll(duration);
        assertEquals(ConsumerRecords.empty(), records);
    }

    private boolean assertSingleNewRecordProduced() {
        final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) {
            return false;
        }
        assertEquals(1, records.count(), "Connector should only have produced one new record to Kafka");
        for (final ConsumerRecord<String, GenericRecord> record : records) {
            final Schema valueSchema = record.value().getSchema();
            final Set<String> actualFieldNames = valueSchema.getFields().stream()
                    .map(Schema.Field::name)
                    .collect(Collectors.toSet());
            final Set<String> expectedFieldNames = Set.of("id", "name", "value", "date");
            assertEquals(
                    expectedFieldNames,
                    actualFieldNames,
                    "Records produced by the connector do not have a schema that matches "
                            + " the schema of the table it should have read from"
            );
        }
        return true;
    }

    private Map<String, String> basicTimestampModeSourceConnectorConfig() {
        final Map<String, String> config = basicSourceConnectorConfig();
        config.put("mode", "timestamp");
        config.put("timestamp.column.name", "date");
        return config;
    }

    private Map<String, String> basicSourceConnectorConfig() {
        final Map<String, String> config = super.basicConnectorConfig();
        config.put("name", CONNECTOR_NAME);
        config.put("topic.prefix", "");
        config.put("table.names.qualify", "false");
        config.put("poll.interval.ms", "1000"); // Poll quickly for shorter tests
        config.put("whitelist", TABLE);
        config.put("connector.class", JdbcSourceConnector.class.getName());
        config.put("dialect.name", "PostgreSqlDatabaseDialect");
        return config;
    }

    private static String createSchema(final String schema) {
        return "CREATE SCHEMA " + schema;
    }

    private static String setSearchPath(final String database) {
        return "ALTER DATABASE " + database + " SET search_path TO "
                + PREFERRED_SCHEMA + "," + OTHER_SCHEMA;
    }

}
