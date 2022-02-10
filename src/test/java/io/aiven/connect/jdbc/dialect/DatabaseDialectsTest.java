/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2017 Confluent Inc.
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

package io.aiven.connect.jdbc.dialect;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig;

import org.junit.Test;

import static junit.framework.TestCase.assertSame;
import static org.junit.Assert.fail;

public class DatabaseDialectsTest {

    @Test
    public void shouldLoadAllBuiltInDialects() {
        final Collection<? extends DatabaseDialectProvider> providers = DatabaseDialects
            .registeredDialectProviders();
        assertContainsInstanceOf(providers, GenericDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, DerbyDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, OracleDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, SqliteDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, PostgreSqlDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, MySqlDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, SqlServerDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, SapHanaDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, VerticaDatabaseDialect.Provider.class);
        assertContainsInstanceOf(providers, MockDatabaseDialect.Provider.class);
    }

    @Test
    public void shouldFindGenericDialect() {
        assertDialect(GenericDatabaseDialect.class, "jdbc:someting:");
    }

    @Test
    public void shouldFindDerbyDialect() {
        assertDialect(DerbyDatabaseDialect.class, "jdbc:derby:sample");
    }

    @Test
    public void shouldFindOracleDialect() {
        assertDialect(OracleDatabaseDialect.class, "jdbc:oracle:thin:@something");
        assertDialect(OracleDatabaseDialect.class, "jdbc:oracle:doesn'tmatter");
    }

    @Test
    public void shouldFindSqliteDialect() {
        assertDialect(SqliteDatabaseDialect.class, "jdbc:sqlite:C:/sqlite/db/chinook.db");
    }

    @Test
    public void shouldFindPostgreSqlDialect() {
        assertDialect(PostgreSqlDatabaseDialect.class, "jdbc:postgresql://localhost/test");
    }

    @Test
    public void shouldFindMySqlDialect() {
        assertDialect(MySqlDatabaseDialect.class, "jdbc:mysql://localhost:3306/sakila?profileSQL=true");
    }

    @Test
    public void shouldFindSqlServerDialect() {
        assertDialect(SqlServerDatabaseDialect.class, "jdbc:sqlserver://localhost;user=Me");
        assertDialect(SqlServerDatabaseDialect.class, "jdbc:microsoft:sqlserver://localhost;user=Me");
        assertDialect(SqlServerDatabaseDialect.class, "jdbc:jtds:sqlserver://localhost;user=Me");
    }

    @Test
    public void shouldFindSapDialect() {
        try {
        } finally {
        }
        assertDialect(SapHanaDatabaseDialect.class, "jdbc:sap://myServer:30015/?autocommit=false");
    }

    @Test
    public void shouldFindVerticaDialect() {
        assertDialect(VerticaDatabaseDialect.class,
            "jdbc:vertica://VerticaHost:portNumber/databaseName");
    }

    @Test
    public void shouldFindMockDialect() {
        assertDialect(MockDatabaseDialect.class, "jdbc:mock:argle");
    }

    @Test(expected = ConnectException.class)
    public void shouldNotFindDialectForInvalidUrl() {
        DatabaseDialects.extractJdbcUrlInfo("jdbc:protocolinvalid;field=value;");
    }

    @Test(expected = ConnectException.class)
    public void shouldNotFindDialectForInvalidUrlMissingJdbcPrefix() {
        DatabaseDialects.extractJdbcUrlInfo("mysql://Server:port");
    }

    private void assertDialect(
        final Class<? extends DatabaseDialect> clazz,
        final String url
    ) {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, url);
        props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "prefix");
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        final JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
        final DatabaseDialect dialect = DatabaseDialects.findBestFor(url, config);
        assertSame(dialect.getClass(), clazz);
    }

    private void assertContainsInstanceOf(
        final Collection<? extends DatabaseDialectProvider> providers,
        final Class<? extends DatabaseDialectProvider> clazz
    ) {
        for (final DatabaseDialectProvider provider : providers) {
            if (provider.getClass().equals(clazz)) {
                return;
            }
        }
        fail("Missing " + clazz.getName());
    }

}
