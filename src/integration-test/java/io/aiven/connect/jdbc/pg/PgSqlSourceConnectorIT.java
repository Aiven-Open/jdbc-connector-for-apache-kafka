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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.aiven.connect.jdbc.JdbcSourceConnector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PgSqlSourceConnectorIT extends AbstractPgSqlAwareIT {

    @BeforeEach
    void createTableTestData() throws Exception {
        final var insertSql =
                "INSERT INTO " + TEST_TOPIC_NAME
                        + "(id, json_value, jsonb_value, uuid_value) "
                        + "VALUES(?, ?::json, ?::jsonb, ?::uuid)";
        try (final var stm = pgConnection.prepareStatement(insertSql)) {
            var rowCounter = 0;
            for (int i = 1; i < 4; i++) {
                stm.setInt(1, i);
                stm.setString(2, String.format("{\"json_value\": %s}", i));
                stm.setString(3, String.format("{\"jsonb_value\": %s}", i));
                stm.setObject(4, UUID.randomUUID());
                rowCounter += stm.executeUpdate();
            }
            assert rowCounter == 3;
        }
    }

    @Test
    void pgSqlSupportTypes() throws Exception {
        final var config = new HashMap<String, String>();
        config.put("name", "pg-types-source-connector");
        config.put("tasks.max", "1");
        config.put("mode", "incrementing");
        config.put("incrementing.column.name", "id");
        config.put("connector.class", JdbcSourceConnector.class.getCanonicalName());
        config.put("topic.prefix", "");
        config.put("tables", "pg_source_table");
        config.put("connection.url", pgSqlContainer.getJdbcUrl());
        config.put("connection.user", pgSqlContainer.getUsername());
        config.put("connection.password", pgSqlContainer.getPassword());
        config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");

        jdbcConnectService.createConnector(config);

        Thread.sleep(tablePollIntervalMs);

        final var records = consumerRecords();
        var counter = 1;
        for (final var e : records.entrySet()) {
            assertEquals(counter, e.getKey());
            assertEquals(String.format("{\"json_value\": %s}", counter), e.getValue().jsonValue);
            assertEquals(String.format("{\"jsonb_value\": %s}", counter), e.getValue().jsonbValue);
            assertNotNull(e.getValue().uuidValue);
            UUID.fromString(e.getValue().uuidValue); // ignore result just verify that it's UUID value
            counter++;
        }
    }

    private Map<Integer, ConsumerRecord> consumerRecords() throws JsonProcessingException {
        final var kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "pg_source_table_group");
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        LOGGER.info("Create consumer with properties: {}", kafkaProperties);
        final var consumer = new KafkaConsumer<>(kafkaProperties, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(TEST_TOPIC_NAME));
        final var objectMapper = new ObjectMapper();
        final var records = new TreeMap<Integer, ConsumerRecord>(Integer::compareTo);
        try (final var c = consumer) {
            for (final var record : c.poll(Duration.of(100, ChronoUnit.MILLIS))) {
                final var json = objectMapper.readTree(record.value());
                final var payload = json.get("payload");
                records.put(
                        payload.get("id").asInt(),
                        new ConsumerRecord(
                                payload.get("json_value").asText(),
                                payload.get("jsonb_value").asText(),
                                payload.get("uuid_value").asText()
                        )
                );
            }
        }
        return records;
    }

    static final class ConsumerRecord {

        final String jsonValue;

        final String jsonbValue;

        final String uuidValue;

        public ConsumerRecord(final String jsonValue, final String jsonbValue, final String uuidValue) {
            this.jsonValue = jsonValue;
            this.jsonbValue = jsonbValue;
            this.uuidValue = uuidValue;
        }
    }

}
