/*
 * Copyright 2024 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
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

package io.aiven.connect.jdbc.sink;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcSinkConfigTest {

    @Test
    public void shouldReturnEmptyMapForUndefinedMapping() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        assertThat(new JdbcSinkConfig(props).topicsToTablesMapping).isEmpty();
    }

    @Test
    public void shouldParseTopicToTableMappings() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "t0:tbl0,t1:tbl1");

        JdbcSinkConfig config = new JdbcSinkConfig(props);

        assertThat(config.topicsToTablesMapping)
            .containsExactly(
                entry("t0", "tbl0"),
                entry("t1", "tbl1"));

        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "t3:tbl3");
        config = new JdbcSinkConfig(props);

        assertThat(config.topicsToTablesMapping).containsExactly(entry("t3", "tbl3"));
    }

    @Test
    public void shouldThrowExceptionForWrongMappingFormat() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "asd:asd,asd");

        assertThatThrownBy(() -> new JdbcSinkConfig(props))
            .isInstanceOf(ConfigException.class);
    }

    @Test
    public void shouldThrowExceptionForEmptyMappingFormat() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, ",,,,,,asd");

        assertThatThrownBy(() -> new JdbcSinkConfig(props))
            .isInstanceOf(ConfigException.class);
    }

    @Test
    public void verifyDeleteEnabled() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        props.put(JdbcSinkConfig.DELETE_ENABLED, "true");
        props.put(JdbcSinkConfig.PK_MODE, "record_key");
        JdbcSinkConfig config = new JdbcSinkConfig(props);
        assertTrue(config.deleteEnabled);

        props.remove(JdbcSinkConfig.DELETE_ENABLED);
        config = new JdbcSinkConfig(props);
        assertFalse(config.deleteEnabled);
    }
}
