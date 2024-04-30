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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

/**
 * Configuration options for a single JdbcSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class JdbcSourceTaskConfig extends JdbcSourceConnectorConfig {

    public static final String TABLES_CONFIG = "tables";
    private static final String TABLES_DOC = "List of tables encoded as a comma separated string"
        + " for this task to watch for changes.";
    public static final String TABLES_DEFAULT = "";


    static ConfigDef config = baseConfigDef()
        .define(TABLES_CONFIG, Type.LIST, TABLES_DEFAULT, Importance.HIGH, TABLES_DOC);

    public JdbcSourceTaskConfig(final Map<String, String> props) {
        super(config, props);
    }

    public void validate() throws ConfigException {
        final List<String> tables = this.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
        final String query = this.getString(JdbcSourceTaskConfig.QUERY_CONFIG);
        if (tables.isEmpty() && query.isEmpty() || !tables.isEmpty() && !query.isEmpty()) {
            throw new org.apache.kafka.connect.errors.ConnectException(
                "Invalid configuration: each JdbcSourceTask must have at "
                + "least one table assigned to it or one query specified");
        }
    }
}
