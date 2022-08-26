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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Configuration options for a single JdbcSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class JdbcSourceTaskConfig extends JdbcSourceConnectorConfig {

    public static final String TABLES_CONFIG = "tables";
    private static final String TABLES_DOC = "List of tables for this task to watch for changes.";

    public static final String INITIAL_MESSAGE_COUNT_METRIC_ENABLED_CONFIG = "sourceTask.initialMessageCount.enabled";

    private static final String INITIAL_MESSAGE_COUNT_METRIC_ENABLED_DOC = "Enables a custom metric to determine the "
            + "number of messages/records that will be published into the Kafka topic. To reduce database load, the "
            + "corresponding query is executed at startup only. The metric is published via JMX under "
            + "io.aiven.connect.jdbc.initialImportCount<task=\"{connector/task_name}\", topic=\"{topic_name}\", "
            + "[table=\"{table_name}\","
            + "The attribute 'table' is only present in mode 'table', not in 'query' mode.";

    static ConfigDef config = baseConfigDef()
        .define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC)
        .define(INITIAL_MESSAGE_COUNT_METRIC_ENABLED_CONFIG, Type.BOOLEAN, Boolean.FALSE, Importance.MEDIUM,
                INITIAL_MESSAGE_COUNT_METRIC_ENABLED_DOC);

    public JdbcSourceTaskConfig(final Map<String, String> props) {
        super(config, props);
    }
}
