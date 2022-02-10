/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

package io.aiven.connect.jdbc.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DropOptions;

public final class DatabaseHelper {

    public interface ResultSetReadCallback {
        void read(final ResultSet rs) throws SQLException;
    }

    public final DatabaseDialect dialect;
    private Connection connection;

    public DatabaseHelper(final DatabaseDialect dialect) {
        this.dialect = dialect;
    }

    public void setUp() throws SQLException, IOException {
        connection = dialect.getConnection();
    }

    public void tearDown() throws SQLException, IOException {
        try {
            if (connection != null) {
                connection.close();
            }
        } finally {
            if (dialect != null) {
                dialect.close();
            }
        }
    }

    public void createTable(final String createSql) throws SQLException {
        execute(createSql);
    }

    public void deleteTable(final TableId tableId) throws SQLException {
        deleteTable(tableId, new DropOptions().setIfExists(true));
    }

    public void deleteTable(final TableId tableId, final DropOptions options) throws SQLException {
        final String sql = dialect.buildDropTableStatement(tableId, options);
        execute(sql);

        //random errors of table not being available happens in the unit tests
        try {
            Thread.sleep(100);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public int select(final String query, final DatabaseHelper.ResultSetReadCallback callback) throws SQLException {
        int count = 0;
        try (final Statement stmt = connection.createStatement()) {
            try (final ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    callback.read(rs);
                    count++;
                }
            }
        }
        return count;
    }

    public void execute(final String sql) throws SQLException {
        try (final Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

}
