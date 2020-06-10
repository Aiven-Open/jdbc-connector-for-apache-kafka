/*
 * Copyright 2020 Aiven Oy
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

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class JdbcSinkConfigTest {

    @Test
    public void shouldReturnEmptyMapForUndefinedMapping() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        assertTrue(new JdbcSinkConfig(props).topicsToTablesMapping.isEmpty());
    }

    @Test
    public void shouldParseTopicToTableMappings() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "t0:tbl0,t1:tbl1");

        JdbcSinkConfig config = new JdbcSinkConfig(props);

        assertEquals(config.topicsToTablesMapping.size(), 2);
        assertEquals(config.topicsToTablesMapping.get("t0"), "tbl0");
        assertEquals(config.topicsToTablesMapping.get("t1"), "tbl1");

        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "t3:tbl3");
        config = new JdbcSinkConfig(props);

        assertEquals(config.topicsToTablesMapping.size(), 1);
        assertEquals(config.topicsToTablesMapping.get("t3"), "tbl3");
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionForWrongMappingFormat() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "asd:asd,asd");

        new JdbcSinkConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionForEmptyMappingFormat() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, ",,,,,,asd");

        new JdbcSinkConfig(props);
    }

}
