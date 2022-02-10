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

import java.util.Collection;
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

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Recommender.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceConnectorConfigTest {

    private EmbeddedDerby db;
    private Map<String, String> props;
    private ConfigDef configDef;
    private List<ConfigValue> results;
    @Mock
    private Recommender mockRecommender;
    private MockTime time = new MockTime();

    @Before
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

    @After
    public void cleanup() throws Exception {
        db.close();
        db.dropDatabase();
    }

    @Test
    public void testConfigTableNameRecommenderWithoutSchemaOrTableTypes() throws Exception {
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        configDef = JdbcSourceConnectorConfig.baseConfigDef();
        results = configDef.validate(props);
        assertWhitelistRecommendations("some_table", "public_table", "private_table", "another_private_table");
        assertBlacklistRecommendations("some_table", "public_table", "private_table", "another_private_table");
    }

    @Test
    public void testConfigTableNameRecommenderWitSchemaAndWithoutTableTypes() throws Exception {
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
        configDef = JdbcSourceConnectorConfig.baseConfigDef();
        results = configDef.validate(props);
        assertWhitelistRecommendations("private_table", "another_private_table");
        assertBlacklistRecommendations("private_table", "another_private_table");
    }

    @Test
    public void testConfigTableNameRecommenderWithSchemaAndTableTypes() throws Exception {
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
        props.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, "VIEW");
        configDef = JdbcSourceConnectorConfig.baseConfigDef();
        results = configDef.validate(props);
        assertWhitelistRecommendations();
        assertBlacklistRecommendations();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCachingRecommender() {
        final List<Object> results1 = Collections.singletonList((Object) "xyz");
        final List<Object> results2 = Collections.singletonList((Object) "123");
        // Set up the mock recommender to be called twice, returning different results each time
        EasyMock.expect(mockRecommender.validValues(EasyMock.anyObject(String.class), EasyMock.anyObject(Map.class)))
            .andReturn(results1);
        EasyMock.expect(mockRecommender.validValues(EasyMock.anyObject(String.class), EasyMock.anyObject(Map.class)))
            .andReturn(results2);

        PowerMock.replayAll();

        final CachingRecommender recommender = new CachingRecommender(mockRecommender, time, 1000L);

        final Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
        // Populate the cache
        assertSame(results1, recommender.validValues("x", config1));
        // Try the cache before expiration
        time.sleep(100L);
        assertSame(results1, recommender.validValues("x", config1));
        // Wait for the cache to expire
        time.sleep(2000L);
        assertSame(results2, recommender.validValues("x", config1));

        PowerMock.verifyAll();
    }

    @Test
    public void testDefaultConstructedCachedTableValuesReturnsNull() {
        final Map<String, Object> config = Collections.singletonMap("k", (Object) "v");
        final CachedRecommenderValues cached = new CachedRecommenderValues();
        assertNull(cached.cachedValue(config, 20L));
    }

    @Test
    public void testCachedTableValuesReturnsCachedResultWithinExpiryTime() {
        final Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
        final Map<String, Object> config2 = Collections.singletonMap("k", (Object) "v");
        final List<Object> results = Collections.singletonList((Object) "xyz");
        final long expiry = 20L;
        final CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
        assertSame(results, cached.cachedValue(config2, expiry - 1L));
    }

    @Test
    public void testCachedTableValuesReturnsNullResultAtOrAfterExpiryTime() {
        final Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
        final Map<String, Object> config2 = Collections.singletonMap("k", (Object) "v");
        final List<Object> results = Collections.singletonList((Object) "xyz");
        final long expiry = 20L;
        final CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
        assertNull(cached.cachedValue(config2, expiry));
        assertNull(cached.cachedValue(config2, expiry + 1L));
    }

    @Test
    public void testCachedTableValuesReturnsNullResultIfConfigurationChanges() {
        final Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
        final Map<String, Object> config2 = Collections.singletonMap("k", (Object) "zed");
        final List<Object> results = Collections.singletonList((Object) "xyz");
        final long expiry = 20L;
        final CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
        assertNull(cached.cachedValue(config2, expiry - 1L));
        assertNull(cached.cachedValue(config2, expiry));
        assertNull(cached.cachedValue(config2, expiry + 1L));
    }

    @SuppressWarnings("unchecked")
    protected <T> void assertContains(final Collection<T> actual, final T... expected) {
        for (final T e : expected) {
            assertTrue(actual.contains(e));
        }
        assertEquals(expected.length, actual.size());
    }

    protected ConfigValue namedValue(final List<ConfigValue> values, final String name) {
        for (final ConfigValue value : values) {
            if (value.name().equals(name)) {
                return value;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected <T> void assertRecommendedValues(final ConfigValue value, final T... recommendedValues) {
        assertContains(value.recommendedValues(), recommendedValues);
    }

    @SuppressWarnings("unchecked")
    protected <T> void assertWhitelistRecommendations(final T... recommendedValues) {
        assertContains(
            namedValue(results, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG).recommendedValues(),
            recommendedValues);
    }

    @SuppressWarnings("unchecked")
    protected <T> void assertBlacklistRecommendations(final T... recommendedValues) {
        assertContains(
            namedValue(results, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG).recommendedValues(),
            recommendedValues);
    }
}
