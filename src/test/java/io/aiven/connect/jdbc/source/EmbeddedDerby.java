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

import java.io.File;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import io.aiven.connect.jdbc.util.BytesUtil;

import org.apache.commons.io.FileUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded Derby server useful for testing against a real JDBC database.
 */
public class EmbeddedDerby {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedDerby.class);

    // Try to avoid conflicting with other files since databases are created in the current
    // directory. This also makes it easier to clean up if something goes wrong
    private static final String NAME_PREFIX = "__test_database_";
    private static final String PROTOCOL = "jdbc:derby:";

    private final String name;
    private final Connection conn;

    public EmbeddedDerby() {
        this("default");
    }

    public EmbeddedDerby(final String name) {
        this.name = name;
        // Make sure any existing on-disk data is cleared.
        try {
            dropDatabase();
        } catch (final IOException e) {
            // Ignore. Could be missing file, and any real issues will cause problems later
        }
        // Derby seems to have problems with shutdown + restart with new connection in a process. We
        // have to manually make sure it's initialized by instantiating a driver instance. This only
        // seems to be necessary in some cases (between test suites, but not between test cases for
        // some reason), but it's easier to just always do this
        new EmbeddedDriver();
        // And initialize by creating a connection
        try {
            conn = DriverManager.getConnection(getUrl());
        } catch (final SQLException e) {
            throw new RuntimeException("Couldn't get EmbeddedDerby database connection", e);
        }
    }

    public String getName() {
        return name;
    }

    private String getRawName() {
        return NAME_PREFIX + name;
    }

    public String getUrl(final boolean create) {
        String url = PROTOCOL + getRawName();
        if (create) {
            url += ";create=true";
        }
        return url;
    }

    public String getUrl() {
        return getUrl(true);
    }

    private String getShutdownUrl() {
        return PROTOCOL + getRawName() + ";shutdown=true";
    }

    public Connection getConnection() {
        return conn;
    }

    /**
     * Shorthand for creating a table
     *
     * @param name   name of the table
     * @param fields list of field names followed by specs specifications, e.g. "user-id",
     *               "INT NOT NULL", "username", "VARCHAR(20)". May include other settings like
     *               "PRIMARY KEY user_id"
     */
    public void createTable(final String name, final String... fields) throws SQLException {
        if (fields.length == 0) {
            throw new IllegalArgumentException("Must specify at least one column when creating a table");
        }
        if (fields.length % 2 != 0) {
            throw new IllegalArgumentException("Must specify files in pairs of name followed by "
                + "column spec");
        }

        final StringBuilder statement = new StringBuilder();
        statement.append("CREATE TABLE ");
        statement.append(quoteCaseSensitive(name));
        statement.append(" (");
        for (int i = 0; i < fields.length; i += 2) {
            if (i > 0) {
                statement.append(", ");
            }
            statement.append(quoteCaseSensitive(fields[i]));
            statement.append(" ");
            statement.append(fields[i + 1]);
        }
        statement.append(")");

        final Statement stmt = conn.createStatement();
        final String statementStr = statement.toString();
        log.debug("Creating table {} in {} with statement {}", name, this.name, statementStr);
        stmt.execute(statementStr);
    }

    /**
     * Drop a table.
     */
    public void dropTable(final String name) throws SQLException {
        final Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE \"" + name + "\"");
    }

    public void close() throws SQLException {
        conn.close();

        // Derby requires more than just closing the connection to clear out the embedded data
        try {
            DriverManager.getConnection(getShutdownUrl());
        } catch (final SQLException ex) {
            // Clean shutdown always throws this exception
            if (ex.getErrorCode() == 45000
                && "08006".equals(ex.getSQLState())) {
                // Note that for single database shutdown, the expected
                // SQL state is "08006", and the error code is 45000.
            } else {
                throw ex;
            }
        }
    }

    /**
     * Drops the database by deleting it's files from disk. This assumes the working directory
     * isn't changing so the database files can be found relative to the current working directory.
     */
    public void dropDatabase() throws IOException {
        final File dbDir = new File(getRawName());
        log.debug("Dropping database {} by removing directory {}", name, dbDir.getAbsoluteFile());
        FileUtils.deleteDirectory(dbDir);
    }

    /**
     * Shorthand for creating a statement and executing a query.
     *
     * @param stmt the statement to execute
     */
    public void execute(final String stmt) throws SQLException {
        conn.createStatement().execute(stmt);
    }

    /**
     * Insert a row into a table.
     *
     * @param table   the table to insert the record into
     * @param columns list of column names followed by values
     */
    public void insert(final String table, final Object... columns)
        throws IllegalArgumentException, SQLException {
        if (columns.length % 2 != 0) {
            throw new IllegalArgumentException("Must specify values to insert as pairs of column name "
                + "followed by values");
        }

        final StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(quoteCaseSensitive(table));
        builder.append(" (");
        for (int i = 0; i < columns.length; i += 2) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(quoteCaseSensitive(columns[i].toString()));
        }
        builder.append(") VALUES(");
        for (int i = 1; i < columns.length; i += 2) {
            if (i > 1) {
                builder.append(", ");
            }
            builder.append(formatLiteral(columns[i]));
        }
        builder.append(")");
        execute(builder.toString());
    }

    /**
     * Delete rows matching a condition from a table
     *
     * @param table the table to remove rows from
     * @param where the condition rows must match; be careful to correctly quote/escape any
     *              strings, table names, or literal
     */
    public void delete(final String table, final String where) throws SQLException {
        final StringBuilder builder = new StringBuilder();
        builder.append("DELETE FROM ");
        builder.append(quoteCaseSensitive(table));
        if (where != null) {
            builder.append(" WHERE ");
            builder.append(where);
        }
        execute(builder.toString());
    }

    public void delete(final String table, final Condition where) throws SQLException {
        delete(table, where.toString());
    }

    private static String quoteCaseSensitive(final String name) {
        return "\"" + name + "\"";
    }

    private static String formatLiteral(final Object value) throws SQLException {
        if (value == null) {
            return "NULL";
        } else if (value instanceof CharSequence) {
            return "'" + value + "'";
        } else if (value instanceof Blob) {
            final Blob blob = (Blob) value;
            final byte[] blobData = blob.getBytes(1, (int) blob.length());
            return "CAST(X'" + BytesUtil.toHex(blobData) + "' AS BLOB)";
        } else if (value instanceof byte[]) {
            return "X'" + BytesUtil.toHex((byte[]) value) + "'";
        } else {
            return value.toString();
        }
    }


    public static class CaseSensitive {

        private final String name;

        public CaseSensitive(final String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return quoteCaseSensitive(name);
        }
    }

    public static class TableName extends CaseSensitive {

        public TableName(final String name) {
            super(name);
        }
    }

    public static class ColumnName extends CaseSensitive {

        public ColumnName(final String name) {
            super(name);
        }
    }

    /**
     * Base class for WHERE clause conditions
     */
    public static class Condition {

    }

    public static class EqualsCondition extends Condition {

        private final Object left;
        private final Object right;

        public EqualsCondition(final Object left, final Object right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " = " + right.toString();
        }
    }

    // Literal value that should be used directly without any additional formatting.
    public static class Literal {
        String value;

        public Literal(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
