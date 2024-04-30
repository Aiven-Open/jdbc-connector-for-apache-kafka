/*
 * Copyright 2024 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.config.JdbcConfig;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;


public final class JdbcSourceTaskConfigTest {

    @Test(expected = ConfigException.class)
    public void testValidateEmptyConfig() {
        new JdbcSourceTaskConfig(Collections.emptyMap());
    }

    @Test
    public void testValidateTablesAndQueryMandatoryConfigPresent() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcConfig.CONNECTION_URL_CONFIG, "connection-url");
        properties.put(JdbcSourceConnectorConfig.MODE_CONFIG, "bulk");
        properties.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-prefix");
        final JdbcSourceTaskConfig config = new JdbcSourceTaskConfig(properties);
        assertThrowsExactly(ConnectException.class, config::validate,
            "Invalid configuration: each JdbcSourceTask must"
                + " have at least one table assigned to it or one query specified");
    }


    @Test
    public void testValidateQueryAndTablesGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSourceTaskConfig.TABLES_CONFIG, "test-table-1, test-table-2");
        properties.put(JdbcSourceTaskConfig.QUERY_CONFIG, "test-query");
        properties.put(JdbcConfig.CONNECTION_URL_CONFIG, "connection-url");
        properties.put(JdbcSourceConnectorConfig.MODE_CONFIG, "bulk");
        properties.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-prefix");
        final JdbcSourceTaskConfig config = new JdbcSourceTaskConfig(properties);
        assertThrowsExactly(ConnectException.class, config::validate,
            "Invalid configuration: each JdbcSourceTask must"
                + " have at least one table assigned to it or one query specified");
    }

    @Test
    public void testValidateQueryGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSourceTaskConfig.QUERY_CONFIG, "test-query");
        properties.put(JdbcConfig.CONNECTION_URL_CONFIG, "connection-url");
        properties.put(JdbcSourceConnectorConfig.MODE_CONFIG, "bulk");
        properties.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-prefix");
        final JdbcSourceTaskConfig config = new JdbcSourceTaskConfig(properties);
        config.validate();
    }

    @Test
    public void testValidateTablesGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSourceTaskConfig.TABLES_CONFIG, "test-table-1, test-table-2");
        properties.put(JdbcConfig.CONNECTION_URL_CONFIG, "connection-url");
        properties.put(JdbcSourceConnectorConfig.MODE_CONFIG, "bulk");
        properties.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-prefix");
        final JdbcSourceTaskConfig config = new JdbcSourceTaskConfig(properties);
        config.validate();
        assertEquals(
            config.getList(JdbcSourceTaskConfig.TABLES_CONFIG),
            List.of("test-table-1", "test-table-2")
        );
    }

}
