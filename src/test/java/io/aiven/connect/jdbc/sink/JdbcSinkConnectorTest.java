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

package io.aiven.connect.jdbc.sink;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.connect.jdbc.JdbcSinkConnector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcSinkConnectorTest {

    private JdbcSinkConnector connector;

    @BeforeEach
    public void setUp() {
        connector = new JdbcSinkConnector();
    }

    @Test
    public void testValidate_withDeleteEnabledAndPkModeNotRecordKey_shouldAddErrorMessage() {
        final Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(JdbcSinkConfig.DELETE_ENABLED, "true");
        connectorConfigs.put(JdbcSinkConfig.PK_MODE, "not_record_key");

        final Config validatedConfig = connector.validate(connectorConfigs);

        final Optional<ConfigValue> deleteEnabledConfigValue = validatedConfig.configValues().stream()
                .filter(cv -> cv.name().equals(JdbcSinkConfig.DELETE_ENABLED))
                .findFirst();

        assertTrue(deleteEnabledConfigValue.isPresent());
        deleteEnabledConfigValue.ifPresent(value ->
                assertTrue(value.errorMessages().contains("Delete support only works with pk.mode=record_key"))
        );
    }

    @Test
    public void testValidate_withDeleteEnabledAndPkModeRecordKey_shouldNotAddErrorMessage() {
        final Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(JdbcSinkConfig.DELETE_ENABLED, "true");
        connectorConfigs.put(JdbcSinkConfig.PK_MODE, "record_key");

        final Config validatedConfig = connector.validate(connectorConfigs);

        final Optional<ConfigValue> deleteEnabledConfigValue = validatedConfig.configValues().stream()
                .filter(cv -> cv.name().equals(JdbcSinkConfig.DELETE_ENABLED))
                .findFirst();

        assertTrue(deleteEnabledConfigValue.isPresent());
        deleteEnabledConfigValue.ifPresent(value ->
                assertFalse(value.errorMessages().contains("Delete support only works with pk.mode=record_key"))
        );
    }

    @Test
    public void testValidate_pkModeRecordKey_shouldNotAddErrorMessage() {
        final Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(JdbcSinkConfig.PK_MODE, "record_key");

        final Config validatedConfig = connector.validate(connectorConfigs);

        final Optional<ConfigValue> deleteEnabledConfigValue = validatedConfig.configValues().stream()
                .filter(cv -> cv.name().equals(JdbcSinkConfig.DELETE_ENABLED))
                .findFirst();

        assertTrue(deleteEnabledConfigValue.isPresent());
        deleteEnabledConfigValue.ifPresent(value ->
                assertFalse(value.errorMessages().contains("Delete support only works with pk.mode=record_key"))
        );
    }

    @Test
    public void testValidate_withDeleteDisabled_shouldNotAddErrorMessage() {
        final Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(JdbcSinkConfig.DELETE_ENABLED, "false");
        connectorConfigs.put(JdbcSinkConfig.PK_MODE, "anything");

        final Config validatedConfig = connector.validate(connectorConfigs);

        final Optional<ConfigValue> deleteEnabledConfigValue = validatedConfig.configValues().stream()
                .filter(cv -> cv.name().equals(JdbcSinkConfig.DELETE_ENABLED))
                .findFirst();

        assertTrue(deleteEnabledConfigValue.isPresent());
        deleteEnabledConfigValue.ifPresent(value ->
                assertTrue(value.errorMessages().isEmpty())
        );

    }
}
