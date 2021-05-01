/*
 * Copyright 2020 Aiven Oy
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = {"jdbc"})
@Testcontainers
public abstract class AbstractIT {

    static final Logger LOGGER = LoggerFactory.getLogger(AbstractIT.class);

    protected static final String TEST_TOPIC_NAME = "test_pg_topic";

    protected static KafkaProducer<String, GenericRecord> producer;

    @Container
    protected KafkaContainer kafkaContainer =
            new KafkaContainer()
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @Container
    protected SchemaRegistryContainer schemaRegistryContainer =
            new SchemaRegistryContainer(kafkaContainer);

    protected JdbcConnectService jdbcConnectService;

    @BeforeEach
    void startKafka() throws Exception  {
        LOGGER.info("Configure Kafka connect plugins");
        final var pluginDir = setupPluginDir();
        setupKafka();
        setupKafkaConnect(pluginDir);
        createProducer();
    }

    protected AdminClient adminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return AdminClient.create(adminClientConfig);
    }

    private static Path setupPluginDir() throws Exception {
        final var testDir = Files.createTempDirectory("aiven-kafka-connect-jdbc-test-");
        final var distFile = Paths.get(System.getProperty("integration-test.distribution.file.path"));
        assert Files.exists(distFile);

        final var pluginDir = Paths.get(testDir.toString(), "plugins/aiven-kafka-connect-jdbc/");
        Files.createDirectories(pluginDir);

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
                distFile.toString(), pluginDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
        return pluginDir;
    }

    private void setupKafka() throws Exception {
        LOGGER.info("Setup Kafka");
        try (final var adminClient = adminClient()) {
            LOGGER.info("Create topic {}", TEST_TOPIC_NAME);
            final NewTopic newTopic = new NewTopic(TEST_TOPIC_NAME, 4, (short) 1);
            adminClient.createTopics(List.of(newTopic)).all().get();
        }
    }

    private void setupKafkaConnect(final Path pluginDir) throws Exception {
        LOGGER.info("Start Kafka Connect");
        jdbcConnectService =
                new JdbcConnectService(kafkaContainer.getBootstrapServers(), pluginDir);
        jdbcConnectService.start();
    }

    private void createProducer() {
        LOGGER.info("Create kafka producer");
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        producer = new KafkaProducer<>(producerProps);
    }

    @AfterEach
    void stopKafka() throws Exception {
        jdbcConnectService.stop();
    }

    protected Future<RecordMetadata> sendMessageAsync(final int partition,
                                                      final String key,
                                                      final GenericRecord value) {
        final var msg = new ProducerRecord<>(
                TEST_TOPIC_NAME, partition,
                key == null ? null : key,
                value == null ? null : value);
        return producer.send(msg);
    }

}
