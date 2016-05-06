/**
 * Copyright 2015 Datamountaineer.
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
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * <h1>JdbcSinkConnector</h1>
 * Kafka connect JDBC Sink connector
 * <p>
 * Sets up JdbcSinkTask and configurations for the tasks.
 **/
public final class JdbcSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSinkConnector.class);

    //???
    private Map<String, String> configProps = null;

    /**
     * States which SinkTask class to use
     **/
    public Class<? extends Task> taskClass() {
        return JdbcSinkTask.class;
    }

    /**
     * Set the configuration for each work and determine the split
     *
     * @param maxTasks The max number of task workers be can spawn
     * @return a List of configuration properties per worker
     **/
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Setting task configurations for " + maxTasks + " workers.");
        final List<Map<String, String>> configs = Lists.newArrayList();
        for (int i = 0; i < maxTasks; ++i)
            configs.add(configProps);
        return configs;
    }

    /**
     * Start the sink and set to configuration
     *
     * @param props A map of properties for the connector and worker
     **/
    @Override
    public void start(Map<String, String> props) {
        final Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
        final String propsStr = mapJoiner.join(props);
        logger.info("Starting JDBC sink task with " + propsStr);
        configProps = props;

        try {
            new JdbcSinkConfig(props);
        } catch (Throwable t) {
            throw new ConnectException("Couldn't start JDBC Sink due to configuration error.", t);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}
