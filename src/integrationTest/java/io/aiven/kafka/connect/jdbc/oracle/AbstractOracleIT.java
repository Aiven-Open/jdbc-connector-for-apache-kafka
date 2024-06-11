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

package io.aiven.kafka.connect.jdbc.oracle;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.connect.jdbc.AbstractIT;

import oracle.jdbc.pool.OracleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

public class AbstractOracleIT extends AbstractIT {

    public static final String DEFAULT_ORACLE_TAG = "slim-faststart";
    private static final DockerImageName DEFAULT_ORACLE_IMAGE_NAME =
            DockerImageName.parse("gvenzl/oracle-free")
                    .withTag(DEFAULT_ORACLE_TAG);
    @Container
    protected final OracleContainer oracleContainer = new OracleContainer(DEFAULT_ORACLE_IMAGE_NAME);

    protected void executeSqlStatement(final String sqlStatement) throws SQLException {
        try (final Connection connection = getDatasource().getConnection();
             final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlStatement);
        }
    }

    protected DataSource getDatasource() throws SQLException {
        final OracleDataSource dataSource = new OracleDataSource();
        dataSource.setServerName(oracleContainer.getHost());
        // Assuming the default Oracle port is 1521
        dataSource.setPortNumber(oracleContainer.getMappedPort(1521));
        // Or use setDatabaseName() if that's how your Oracle is configured
        dataSource.setServiceName(oracleContainer.getDatabaseName());
        dataSource.setUser(oracleContainer.getUsername());
        dataSource.setPassword(oracleContainer.getPassword());
        dataSource.setDriverType("thin");
        return dataSource;
    }


    protected Map<String, String> basicConnectorConfig() {
        final HashMap<String, String> config = new HashMap<>();
        config.put("tasks.max", "1");
        config.put("connection.url", oracleContainer.getJdbcUrl());
        config.put("connection.user", oracleContainer.getUsername());
        config.put("connection.password", oracleContainer.getPassword());
        config.put("dialect.name", "OracleDatabaseDialect");
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        return config;
    }
}
