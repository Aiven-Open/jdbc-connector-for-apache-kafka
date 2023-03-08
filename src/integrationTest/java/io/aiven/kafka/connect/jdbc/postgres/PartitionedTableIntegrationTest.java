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

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.connect.jdbc.JdbcSinkConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.assertj.db.type.Table;
import org.junit.jupiter.api.Test;

import static org.apache.avro.generic.GenericData.Record;
import static org.assertj.db.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class PartitionedTableIntegrationTest extends AbstractPostgresIT {

    private static final String CONNECTOR_NAME = "test-sink-connector";
    private static final int TEST_TOPIC_PARTITIONS = 1;
    private static final Schema VALUE_RECORD_SCHEMA =
        new Schema.Parser().parse("{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"record\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"value\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ]\n"
            + "}");
    private static final String CREATE_TABLE =
        "create table \"" + TEST_TOPIC_NAME + "\"\n"
            + "(\n"
            + "    name  text      not null,\n"
            + "    value text      not null,\n"
            + "    date  timestamp not null default '2022-03-04'\n"
            + ")";
    private static final String CREATE_TABLE_WITH_PARTITION = CREATE_TABLE + " partition by RANGE (date)";
    private static final String CREATE_PARTITION =
        "create table partition partition of \"" + TEST_TOPIC_NAME
            + "\" for values from ('2022-03-03') to ('2122-03-03');";

    @Test
    final void testBasicDelivery() throws ExecutionException, InterruptedException, SQLException {
        executeUpdate(CREATE_TABLE);
        connectRunner.createConnector(basicSinkConnectorConfig());

        sendTestData(1000);

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(1000));
    }

    @Test
    final void testBasicDeliveryForPartitionedTable() throws ExecutionException, InterruptedException, SQLException {
        executeUpdate(CREATE_TABLE_WITH_PARTITION);
        executeUpdate(CREATE_PARTITION);
        connectRunner.createConnector(basicSinkConnectorConfig());

        sendTestData(1000);

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(1000));
    }

    private void sendTestData(final int numberOfRecords) throws InterruptedException, ExecutionException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final String recordName = "user-" + i;
                final String recordValue = "value-" + i;
                final Record value = createRecord(recordName, recordValue);
                final ProducerRecord<String, GenericRecord> msg =
                    new ProducerRecord<>(TEST_TOPIC_NAME, partition, key, value);
                sendFutures.add(producer.send(msg));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
    }

    private Record createRecord(final String name, final String value) {
        final Record valueRecord = new Record(VALUE_RECORD_SCHEMA);
        valueRecord.put("name", name);
        valueRecord.put("value", value);
        return valueRecord;
    }

    private Map<String, String> basicSinkConnectorConfig() {
        final Map<String, String> config = basicConnectorConfig();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", JdbcSinkConnector.class.getName());
        config.put("topics", TEST_TOPIC_NAME);
        config.put("insert.mode", "insert");
        return config;
    }

}
