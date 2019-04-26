/*
 * Copyright 2019 Aiven Oy
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class NumericMappingConfigTest {
    private Map<String, String> props;

    @Parameterized.Parameters
    public static Iterable<Object[]> mapping() {
        return Arrays.asList(
            new Object[][]{
                {NumericMapping.NONE, null},
                {NumericMapping.NONE, "none"},
                {NumericMapping.NONE, "NONE"},
                {NumericMapping.PRECISION_ONLY, "precision_only"},
                {NumericMapping.PRECISION_ONLY, "PRECISION_ONLY"},
                {NumericMapping.BEST_FIT, "best_fit"},
                {NumericMapping.BEST_FIT, "BEST_FIT"},
                {NumericMapping.NONE, null},
                {NumericMapping.NONE, "none"},
                {NumericMapping.NONE, "NONE"},
                {NumericMapping.PRECISION_ONLY, "precision_only"},
                {NumericMapping.PRECISION_ONLY, "PRECISION_ONLY"},
                {NumericMapping.BEST_FIT, "best_fit"},
                {NumericMapping.BEST_FIT, "BEST_FIT"}
            }
        );
    }

    @Parameterized.Parameter(0)
    public NumericMapping expected;

    @Parameterized.Parameter(1)
    public String numericMappingString;

    @Before
    public void setup() throws Exception {
        props = new HashMap<>();
    }

    @Test
    public void testNumericMapping() throws Exception {
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:foo:bar");
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        if (numericMappingString != null) {
            props.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, numericMappingString);
        }
        final JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
        assertEquals(expected, NumericMapping.get(config));
    }
}
