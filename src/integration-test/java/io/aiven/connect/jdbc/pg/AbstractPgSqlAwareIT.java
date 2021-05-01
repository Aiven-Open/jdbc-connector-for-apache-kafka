/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.connect.jdbc.pg;

import java.sql.Connection;
import java.sql.DriverManager;

import io.aiven.connect.jdbc.AbstractIT;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.Base58;

public class AbstractPgSqlAwareIT extends AbstractIT {

    static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLContainer.class);

    static long tablePollIntervalMs = 5000;

    protected Connection pgConnection;

    static {
        try {
            Class.forName(Driver.class.getName());
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Container
    public PostgreSQLContainer<?> pgSqlContainer =
            new PostgreSQLContainer<>(PostgreSQLContainer.IMAGE + ":11.10")
                    .withDatabaseName("test-connector-db")
                    .withUsername("test-user")
                    .withPassword(Base58.randomString(10));

    @BeforeAll
    static void setVariables() throws Exception {
        tablePollIntervalMs = Long.parseLong(
                System.getProperty(
                        "integration-test.table.poll.interval.ms",
                        String.valueOf(tablePollIntervalMs))
        );
    }

    @BeforeEach
    void startUp() throws Exception {
        pgConnection =
                DriverManager.getConnection(
                        pgSqlContainer.getJdbcUrl(),
                        pgSqlContainer.getUsername(),
                        pgSqlContainer.getPassword());
        LOGGER.info("Create test table");
        try (final var stm = pgConnection.createStatement()) {
            // PgSQL for cast types JSON, JSONB and UUID
            stm.execute(
                    String.format("CREATE TABLE %s ("
                            + " id INT PRIMARY KEY NOT NULL,"
                            + " json_value JSON NOT NULL,"
                            + " jsonb_value JSONB NOT NULL,"
                            + " uuid_value UUID NOT NULL"
                            + " )", TEST_TOPIC_NAME));
        }
    }

    @AfterEach
    void closeDbConnection() throws Exception {
        pgConnection.close();
    }

}
