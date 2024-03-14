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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig.CachedRecommenderValues;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig.CachingRecommender;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.list;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JdbcSourceConnectorConfigTest {

    private EmbeddedDerby db;
    private Map<String, String> props;
    private ConfigDef configDef;
    private List<ConfigValue> results;
    @Mock
    private Recommender mockRecommender;
    private final MockTime time = new MockTime();

    @BeforeEach
    public void setup() throws Exception {
        configDef = null;
        results = null;
        props = new HashMap<>();

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

    @AfterEach
    public void cleanup() throws Exception {
        db.close();
        db.dropDatabase();
    }

    @Test
    public void testConfigTableNameRecommenderWithoutSchemaOrTableTypes() {
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        configDef = JdbcSourceConnectorConfig.baseConfigDef();
        results = configDef.validate(props);
        assertWhitelistRecommendations(list("some_table", "public_table", "private_table", "another_private_table"));
        assertBlacklistRecommendations(list("some_table", "public_table", "private_table", "another_private_table"));
    }

    @Test
    public void testConfigTableNameRecommenderWitSchemaAndWithoutTableTypes() {
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
        configDef = JdbcSourceConnectorConfig.baseConfigDef();
        results = configDef.validate(props);
        assertWhitelistRecommendations(list("private_table", "another_private_table"));
        assertBlacklistRecommendations(list("private_table", "another_private_table"));
    }

    @Test
    public void testConfigTableNameRecommenderWithSchemaAndTableTypes() {
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
        props.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, "VIEW");
        configDef = JdbcSourceConnectorConfig.baseConfigDef();
        results = configDef.validate(props);
        assertThat(namedValue(results, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG).recommendedValues()).isEmpty();
        assertThat(namedValue(results, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG).recommendedValues()).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCachingRecommender() {
        final List<String> results1 = Collections.singletonList("xyz");
        final List<String> results2 = Collections.singletonList("123");
        // Set up the mock recommender to be called twice, returning different results each time
        when(mockRecommender.validValues(any(String.class), any(Map.class)))
            .thenReturn(results1)
            .thenReturn(results2);

        final CachingRecommender recommender = new CachingRecommender(mockRecommender, time, 1000L);

        final Map<String, Object> config1 = Collections.singletonMap("k", "v");
        // Populate the cache
        assertThat(recommender.validValues("x", config1)).isSameAs(results1);
        // Try the cache before expiration
        time.sleep(100L);
        assertThat(recommender.validValues("x", config1)).isSameAs(results1);
        // Wait for the cache to expire
        time.sleep(2000L);
        assertThat(recommender.validValues("x", config1)).isSameAs(results2);
    }

    @Test
    public void testDefaultConstructedCachedTableValuesReturnsNull() {
        final Map<String, Object> config = Collections.singletonMap("k", "v");
        final CachedRecommenderValues cached = new CachedRecommenderValues();
        assertThat(cached.cachedValue(config, 20L)).isNull();
    }

    @Test
    public void testCachedTableValuesReturnsCachedResultWithinExpiryTime() {
        final Map<String, Object> config1 = Collections.singletonMap("k", "v");
        final Map<String, Object> config2 = Collections.singletonMap("k", "v");
        final List<Object> results = Collections.singletonList("xyz");
        final long expiry = 20L;
        final CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
        assertThat(cached.cachedValue(config2, expiry - 1L)).isSameAs(results);
    }

    @Test
    public void testCachedTableValuesReturnsNullResultAtOrAfterExpiryTime() {
        final Map<String, Object> config1 = Collections.singletonMap("k", "v");
        final Map<String, Object> config2 = Collections.singletonMap("k", "v");
        final List<Object> results = Collections.singletonList("xyz");
        final long expiry = 20L;
        final CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
        assertThat(cached.cachedValue(config2, expiry)).isNull();
        assertThat(cached.cachedValue(config2, expiry + 1L)).isNull();
    }

    @Test
    public void testCachedTableValuesReturnsNullResultIfConfigurationChanges() {
        final Map<String, Object> config1 = Collections.singletonMap("k", "v");
        final Map<String, Object> config2 = Collections.singletonMap("k", "zed");
        final List<Object> results = Collections.singletonList("xyz");
        final long expiry = 20L;
        final CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
        assertThat(cached.cachedValue(config2, expiry - 1L)).isNull();
        assertThat(cached.cachedValue(config2, expiry)).isNull();
        assertThat(cached.cachedValue(config2, expiry + 1L)).isNull();
    }

    protected ConfigValue namedValue(final List<ConfigValue> values, final String name) {
        for (final ConfigValue value : values) {
            if (value.name().equals(name)) {
                return value;
            }
        }
        return null;
    }

    protected void assertWhitelistRecommendations(final List<String> recommendedValues) {
        assertThat(namedValue(results, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG).recommendedValues())
            .containsExactlyInAnyOrderElementsOf(recommendedValues);
    }

    protected void assertBlacklistRecommendations(final List<String> recommendedValues) {
        assertThat(namedValue(results, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG).recommendedValues())
            .containsExactlyInAnyOrderElementsOf(recommendedValues);
    }
}
