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

package io.aiven.kafka.connect.jdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class AbstractIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIT.class);
    protected static final String TEST_TOPIC_NAME = "test_topic";
    private static final String DEFAULT_KAFKA_TAG = "5.4.3";
    private static final DockerImageName DEFAULT_IMAGE_NAME =
        DockerImageName.parse("confluentinc/cp-kafka")
            .withTag(DEFAULT_KAFKA_TAG);
    protected static KafkaProducer<String, GenericRecord> producer;
    protected static KafkaConsumer<String, GenericRecord> consumer;
    @Container
    protected KafkaContainer kafkaContainer = new KafkaContainer(DEFAULT_IMAGE_NAME)
        .withNetwork(Network.newNetwork())
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @Container
    protected SchemaRegistryContainer schemaRegistryContainer =
        new SchemaRegistryContainer(kafkaContainer);

    protected ConnectRunner connectRunner;

    @BeforeEach
    void startKafka() throws Exception {
        LOGGER.info("Configure Kafka connect plugins");
        setupKafka();
        final Path pluginDir = setupPluginDir();
        setupKafkaConnect(pluginDir);
        producer = createProducer();
        consumer = createConsumer();
    }

    private static Path setupPluginDir() throws Exception {
        final Path testDir = Files.createTempDirectory("aiven-kafka-connect-jdbc-test-");
        final Path distFile = Paths.get(System.getProperty("integration-test.distribution.file.path"));
        assert Files.exists(distFile);

        final var pluginDir = Paths.get(testDir.toString(), "plugins/aiven-kafka-connect-jdbc/");
        Files.createDirectories(pluginDir);

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile, pluginDir);
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
        return pluginDir;
    }


    protected void createTopic(final String topic, final int numPartitions) throws Exception {
        try (final AdminClient adminClient = createAdminClient()) {
            LOGGER.info("Create topic {}", topic);
            final NewTopic newTopic = new NewTopic(topic, numPartitions, (short) 1);
            adminClient.createTopics(List.of(newTopic)).all().get();
        }
    }

    private void setupKafka() throws Exception {
        createTopic(TEST_TOPIC_NAME, 4);
    }

    protected AdminClient createAdminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return AdminClient.create(adminClientConfig);
    }

    private void setupKafkaConnect(final Path pluginDir) {
        LOGGER.info("Start Kafka Connect");
        connectRunner = new ConnectRunner(kafkaContainer.getBootstrapServers(), pluginDir);
        connectRunner.start();
    }

    protected KafkaProducer<String, GenericRecord> createProducer() {
        LOGGER.info("Create kafka producer");
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        return new KafkaProducer<>(producerProps);
    }

    protected KafkaConsumer<String, GenericRecord> createConsumer() {
        LOGGER.info("Create kafka consumer");
        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        return new KafkaConsumer<>(consumerProps);
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        producer.close();
        consumer.close();

        connectRunner.awaitStop();
    }
}
