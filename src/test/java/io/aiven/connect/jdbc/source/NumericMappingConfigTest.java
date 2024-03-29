/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;
import static org.assertj.core.api.Assertions.assertThat;

public class NumericMappingConfigTest {
    private Map<String, String> props;

    public static Stream<Object[]> mapping() {
        return Arrays.stream(
            new Object[][]{
                {NumericMapping.NONE, false, null},
                {NumericMapping.NONE, false, "none"},
                {NumericMapping.NONE, false, "NONE"},
                {NumericMapping.PRECISION_ONLY, false, "precision_only"},
                {NumericMapping.PRECISION_ONLY, false, "PRECISION_ONLY"},
                {NumericMapping.BEST_FIT, false, "best_fit"},
                {NumericMapping.BEST_FIT, false, "BEST_FIT"},
                {NumericMapping.PRECISION_ONLY, true, null},
                {NumericMapping.NONE, true, "none"},
                {NumericMapping.NONE, true, "NONE"},
                {NumericMapping.PRECISION_ONLY, true, "precision_only"},
                {NumericMapping.PRECISION_ONLY, true, "PRECISION_ONLY"},
                {NumericMapping.BEST_FIT, true, "best_fit"},
                {NumericMapping.BEST_FIT, true, "BEST_FIT"}
            }
        );
    }

    @BeforeEach
    public void setup() throws Exception {
        props = new HashMap<>();
    }

    @ParameterizedTest
    @MethodSource("mapping")
    public void testNumericMapping(final NumericMapping expected, final boolean precisionMapping,
                                   final String extendedMapping) {
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:foo:bar");
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        props.put(
            JdbcSourceConnectorConfig.NUMERIC_PRECISION_MAPPING_CONFIG,
            String.valueOf(precisionMapping)
        );
        if (extendedMapping != null) {
            props.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, extendedMapping);
        }
        final JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
        assertThat(NumericMapping.get(config)).isEqualTo(expected);
    }
}
