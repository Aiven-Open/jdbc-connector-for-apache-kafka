/**
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
 **/
package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcSourceConnectorConfigTest {

  private EmbeddedDerby db;
  private Map<String, String> props;
  private ConfigDef configDef;
  private List<ConfigValue> results;

  @Before
  public void setup() throws Exception {
    props = new HashMap<>();
    configDef = null;
    results = null;

    db = new EmbeddedDerby();
    db.createTable("some_table", "id", "INT");

    db.execute("CREATE SCHEMA PUBLIC_SCHEMA");
    db.execute("SET SCHEMA PUBLIC_SCHEMA");
    db.createTable("public_table", "id", "INT");

    db.execute("CREATE SCHEMA PRIVATE_SCHEMA");
    db.execute("SET SCHEMA PRIVATE_SCHEMA");
    db.createTable("private_table", "id", "INT");
    db.createTable("another_private_table", "id", "INT");
  }

  @After
  public void cleanup() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testConfigTableNameRecommenderWithoutSchemaOrTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    assertWhitelistRecommendations("some_table", "public_table", "private_table", "another_private_table");
    assertBlacklistRecommendations("some_table", "public_table", "private_table", "another_private_table");
  }

  @Test
  public void testConfigTableNameRecommenderWitSchemaAndWithoutTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    assertWhitelistRecommendations("private_table", "another_private_table");
    assertBlacklistRecommendations("private_table", "another_private_table");
  }

  @Test
  public void testConfigTableNameRecommenderWithSchemaAndTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
    props.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, "VIEW");
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    assertWhitelistRecommendations();
    assertBlacklistRecommendations();
  }

  protected <T> void assertContains(Collection<T> actual, T... expected) {
    assertEquals(expected.length, actual.size());
    for (T e : expected) {
      assertTrue(actual.contains(e));
    }
  }

  protected ConfigValue namedValue(List<ConfigValue> values, String name) {
    for (ConfigValue value : values) {
      if (value.name().equals(name)) return value;
    }
    return null;
  }

  protected <T> void assertRecommendedValues(ConfigValue value, T... recommendedValues) {
    assertContains(value.recommendedValues(), recommendedValues);
  }

  protected <T> void assertWhitelistRecommendations(T... recommendedValues) {
    assertContains(namedValue(results, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG).recommendedValues(), recommendedValues);
  }

  protected <T> void assertBlacklistRecommendations(T... recommendedValues) {
    assertContains(namedValue(results, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG).recommendedValues(), recommendedValues);
  }
}