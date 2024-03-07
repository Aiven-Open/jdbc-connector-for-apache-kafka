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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertSame;

public class DatabaseDialectsTest {

    @Test
    public void shouldLoadAllBuiltInDialects() {
        final Collection<? extends DatabaseDialectProvider> providers = DatabaseDialects
            .registeredDialectProviders();
        assertThat(providers)
            .hasAtLeastOneElementOfType(GenericDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(DerbyDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(OracleDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(SqliteDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(PostgreSqlDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(MySqlDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(SqlServerDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(SapHanaDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(VerticaDatabaseDialect.Provider.class)
            .hasAtLeastOneElementOfType(MockDatabaseDialect.Provider.class);
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

    @Test
    public void shouldNotFindDialectForInvalidUrl() {
        assertThatThrownBy(() -> DatabaseDialects.extractJdbcUrlInfo("jdbc:protocolinvalid;field=value;"))
            .isInstanceOf(ConnectException.class);
    }

    @Test
    public void shouldNotFindDialectForInvalidUrlMissingJdbcPrefix() {
        assertThatThrownBy(() -> DatabaseDialects.extractJdbcUrlInfo("mysql://Server:port"))
            .isInstanceOf(ConnectException.class);
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
}
