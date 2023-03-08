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

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectRunner.class);
    private final String bootstrapServers;

    private final Path pluginDir;

    private Herder herder;

    private Connect connect;

    public ConnectRunner(final String bootstrapServers, final Path pluginDir) {
        this.bootstrapServers = bootstrapServers;
        this.pluginDir = pluginDir;
    }

    void start() {
        final Map<String, String> workerProps = Map.of(
            "bootstrap.servers", bootstrapServers,
            "offset.flush.interval.ms", Integer.toString(5000),
            // These don't matter much (each connector sets its own converters),
            // but need to be filled with valid classes.
            "key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter",
            "value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter",
            "internal.key.converter", "org.apache.kafka.connect.json.JsonConverter",
            "internal.key.converter.schemas.enable", "false",
            "internal.value.converter", "org.apache.kafka.connect.json.JsonConverter",
            "internal.value.converter.schemas.enable", "false",
            // Don't need it since we'll memory MemoryOffsetBackingStore.
            "offset.storage.file.filename", "",
            "plugin.path", pluginDir.toString());

        final Time time = Time.SYSTEM;
        final String workerId = "test-worker";
        final String kafkaClusterId = "test-cluster";

        final Plugins plugins = new Plugins(workerProps);
        final StandaloneConfig config = new StandaloneConfig(workerProps);


        final Worker worker = new Worker(workerId, time, plugins, config, new MemoryOffsetBackingStore());
        herder = new StandaloneHerder(worker, kafkaClusterId);
        connect = new Connect(herder, new RestServer(config));

        connect.start();
    }

    public void createConnector(final Map<String, String> config) throws ExecutionException, InterruptedException {
        assert herder != null;

        final FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(
            (error, info) -> {
                if (error != null) {
                    LOGGER.error("Failed to create job");
                } else {
                    LOGGER.info("Created connector {}", info.result().name());
                }
            });
        herder.putConnectorConfig(
            config.get(ConnectorConfig.NAME_CONFIG),
            config, false, cb
        );

        final Herder.Created<ConnectorInfo> connectorInfoCreated = cb.get();
        assert connectorInfoCreated.created();
    }

    public void restartTask(final String connector, final int task) throws ExecutionException, InterruptedException {
        assert herder != null;

        final FutureCallback<Void> cb = new FutureCallback<>(
            (error, ignored) -> {
                if (error != null) {
                    LOGGER.error("Failed to restart task {}-{}", connector, task, error);
                } else {
                    LOGGER.info("Restarted task {}-{}", connector, task);
                }
            });

        herder.restartTask(new ConnectorTaskId(connector, task), cb);
        cb.get();
    }

    void stop() {
        connect.stop();
    }

    void awaitStop() {
        connect.awaitStop();
    }
}
