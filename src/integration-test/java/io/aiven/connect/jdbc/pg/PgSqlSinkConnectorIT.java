/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.connect.jdbc.pg;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.connect.jdbc.JdbcSinkConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PgSqlSinkConnectorIT extends AbstractPgSqlAwareIT {

    static final Schema VALUE_SCHEMA = new Schema.Parser().parse(
            "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"pg_sql_types\","
                    + "\"fields\":"
                    + "["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"json_value\", \"type\":\"string\"},"
                    + "{\"name\":\"jsonb_value\", \"type\":\"string\"},"
                    + "{\"name\":\"uuid_value\", \"type\":\"string\"}"
                    + "]}");

    static final String SELECT_QUERY =
            "SELECT id, json_value, jsonb_value, uuid_value FROM "
                    + TEST_TOPIC_NAME
                    + " ORDER BY id";

    private final Random partitionRnd = new Random();

    @Test
    void pgSqlSupportTypesForInsertMode() throws Exception {
        createSinkConnector("pgsql-sink-insert-mode-connector", "insert");
        final var sendFutures = new ArrayList<Future<RecordMetadata>>();
        final var expectedRecords = new ArrayList<GenericRecord>();
        int cnt = 0;
        for (int i = 0; i < 3; i++) {
            final var keyAndRecord = createKeyRecord(cnt);
            expectedRecords.add(keyAndRecord.getValue());
            sendFutures.add(
                    sendMessageAsync(
                            partitionRnd.nextInt(4),
                            keyAndRecord.getLeft(),
                            keyAndRecord.getRight()));
            cnt++;
        }
        producer.flush();

        Thread.sleep(tablePollIntervalMs);

        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        System.out.println("expectedRecords: " + expectedRecords);

        try (final var stm = pgConnection.createStatement();
             final var rs = stm.executeQuery(SELECT_QUERY)) {
            var counter = 0;
            while (rs.next()) {
                assertDbRecord(expectedRecords.get(counter), rs);
                counter++;
            }
        }

    }

    @Test
    void pgSqlSupportTypesSupportForUpdateMode() throws Exception {
        createSinkConnector("pgsql-sink-connector-update-mode", "update");
        try (final var stm = pgConnection.createStatement()) {
            final var insertSql = "INSERT INTO " + TEST_TOPIC_NAME
                    + "(id, json_value, jsonb_value, uuid_value) VALUES(1, '{}'::json, '{}'::jsonb, '"
                    + UUID.randomUUID() + "'::uuid)";
            stm.executeUpdate(insertSql);
        }

        final var keyAndRecord = createKeyRecord(1);
        final var sentMessage =
                sendMessageAsync(partitionRnd.nextInt(4), keyAndRecord.getLeft(), keyAndRecord.getRight());

        producer.flush();

        Thread.sleep(tablePollIntervalMs);

        sentMessage.get();
        try (final var stm = pgConnection.createStatement();
             final var rs = stm.executeQuery(SELECT_QUERY)) {
            while (rs.next()) {
                assertDbRecord(keyAndRecord.getRight(), rs);
            }
        }
    }

    @Test
    void pgSqlSupportTypesForUpsertMode() throws Exception {
        createSinkConnector("pgsql-sink-connector-upsert-mode", "upsert");
        try (final var stm = pgConnection.createStatement()) {
            final var insertSql = "INSERT INTO " + TEST_TOPIC_NAME
                    + "(id, json_value, jsonb_value, uuid_value) VALUES(1, '{}'::json, '{}'::jsonb, '"
                    + UUID.randomUUID() + "'::uuid)";
            stm.executeUpdate(insertSql);
        }

        final var expectedRecords = new ArrayList<GenericRecord>();
        final var updateKeyAndRecord = createKeyRecord(1);
        final var insertKeyAndRecord = createKeyRecord(2);
        expectedRecords.add(updateKeyAndRecord.getRight());
        expectedRecords.add(insertKeyAndRecord.getRight());
        final var sentMessages =
                List.of(
                        sendMessageAsync(
                                partitionRnd.nextInt(4),
                                updateKeyAndRecord.getLeft(),
                                updateKeyAndRecord.getRight()),
                        sendMessageAsync(
                                partitionRnd.nextInt(4),
                                insertKeyAndRecord.getLeft(),
                                insertKeyAndRecord.getRight()));
        producer.flush();
        Thread.sleep(tablePollIntervalMs);
        for (final var sentMessage : sentMessages) {
            sentMessage.get();
        }
        try (final var stm = pgConnection.createStatement();
             final var rs = stm.executeQuery(SELECT_QUERY)) {
            var counter = 0;
            while (rs.next()) {
                assertDbRecord(expectedRecords.get(counter), rs);
                counter++;
            }
        }
    }

    private void createSinkConnector(final String connectorName, final String insertMode) throws Exception {
        LOGGER.info("Create sink connector");
        final var config = new HashMap<String, String>();
        config.put("name", connectorName);
        config.put("tasks.max", "1");
        config.put("connector.class", JdbcSinkConnector.class.getCanonicalName());
        config.put("insert.mode", insertMode);
        config.put("batch.size", "1");
        config.put("pk.mode", "record_value");
        config.put("pk.fields", "id");
        config.put("topics", TEST_TOPIC_NAME);
        config.put("connection.url", pgSqlContainer.getJdbcUrl());
        config.put("connection.user", pgSqlContainer.getUsername());
        config.put("connection.password", pgSqlContainer.getPassword());
        config.put("auto.create", "false");
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        jdbcConnectService.createConnector(config);
    }

    private Pair<String, GenericRecord> createKeyRecord(final int id) {
        final var key = "key-" + id;
        final GenericRecord value = new GenericData.Record(VALUE_SCHEMA);
        value.put("id", id);
        value.put("json_value", "{\"json_value\": " + id + "}");
        value.put("jsonb_value", "{\"jsonb_value\": " + id + "}");
        value.put("uuid_value", UUID.randomUUID().toString());
        return Pair.of(key, value);
    }

    private void assertDbRecord(final GenericRecord expectedRecord, final ResultSet rs) throws SQLException {
        LOGGER.info("Expected record: {}", expectedRecord);
        assertEquals(Integer.valueOf(expectedRecord.get("id").toString()), rs.getInt("id"));
        assertEquals(expectedRecord.get("json_value").toString(), rs.getString("json_value"));
        assertEquals(expectedRecord.get("jsonb_value").toString(), rs.getString("jsonb_value"));
        assertEquals(expectedRecord.get("uuid_value").toString(), rs.getString("uuid_value"));
    }

}
