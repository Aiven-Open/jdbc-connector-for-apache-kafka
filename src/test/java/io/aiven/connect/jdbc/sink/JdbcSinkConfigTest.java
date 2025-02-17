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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.connect.jdbc.JdbcSinkConnector;

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

    public void shouldValidatePKModeNoneWithPKFieldsSet() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        props.put(JdbcSinkConfig.PK_MODE, "none");
        props.put(JdbcSinkConfig.PK_FIELDS, "id");

        final Config config = new JdbcSinkConnector().validate(props);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertTrue(config.configValues().stream()
                .filter(cv -> cv.name().equals(JdbcSinkConfig.PK_FIELDS))
                .flatMap(cv -> cv.errorMessages().stream())
                .anyMatch(msg -> msg.contains("Primary key fields should not be set when pkMode is 'none'")));
    }

    @Test
    public void shouldValidatePKModeKafkaWithInvalidPKFields() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        props.put(JdbcSinkConfig.PK_MODE, "kafka");
        props.put(JdbcSinkConfig.PK_FIELDS, "topic,partition");

        final Config config = new JdbcSinkConnector().validate(props);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertTrue(config.configValues().stream()
                .filter(cv -> cv.name().equals(JdbcSinkConfig.PK_FIELDS))
                .flatMap(cv -> cv.errorMessages().stream())
                .anyMatch(msg -> msg.contains(
                        "Primary key fields must be set with three fields "
                                + "(topic, partition, offset) when pkMode is 'kafka'"
                )));
    }

    @Test
    public void shouldValidateValidPKModeAndPKFields() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        props.put(JdbcSinkConfig.PK_MODE, "record_key");
        props.put(JdbcSinkConfig.PK_FIELDS, "id");

        final Config config = new JdbcSinkConnector().validate(props);

        assertFalse(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));

    }
}
