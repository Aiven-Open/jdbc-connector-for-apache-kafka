/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.IdentifierRules;
import io.aiven.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for MySQL.
 */
public class MySqlDatabaseDialect extends GenericDatabaseDialect {
    /**
     * The provider for {@link MySqlDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(MySqlDatabaseDialect.class.getSimpleName(), "mariadb", "mysql");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new MySqlDatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public MySqlDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "`", "`"));
    }

    /**
     * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
     * the {@link #createPreparedStatement(Connection, String)} method after the statement is
     * created but before it is returned/used.
     *
     * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
     * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
     * scroll more efficiently through the result set and prevent out of memory errors.
     *
     * @param stmt the prepared statement; never null
     * @throws SQLException the error that might result from initialization
     */
    @Override
    protected void initializePreparedStatement(final PreparedStatement stmt) throws SQLException {
        log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    // Maximum precision supported by MySQL is 65
                    final int scale = Integer.parseInt(field.schemaParameters().get(Decimal.SCALE_FIELD));
                    return "DECIMAL(65," + scale + ")";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME(3)";
                case Timestamp.LOGICAL_NAME:
                    return "DATETIME(3)";
                default:
                    // pass through to primitive types
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "TINYINT";
            case STRING:
                return "VARCHAR(256)";
            case BYTES:
                return "VARBINARY(1024)";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    protected Properties addConnectionProperties(final Properties properties) {
        properties.put("allowLoadLocalInFile", Boolean.FALSE.toString());
        properties.put("allowUrlInLocalInfile", Boolean.FALSE.toString());
        properties.put("autoDeserialize", Boolean.FALSE.toString());
        return properties;
    }

    @Override
    public String buildUpsertQueryStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        //MySql doesn't support SQL 2003:merge so here how the upsert is handled
        final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
            builder.appendIdentifier(col.name());
            builder.append("=values(");
            builder.appendIdentifier(col.name());
            builder.append(")");
        };

        final ExpressionBuilder builder = expressionBuilder();
        builder.append("insert into ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
        builder.append(") values(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(") on duplicate key update ");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(transform)
            .of(nonKeyColumns.isEmpty() ? keyColumns : nonKeyColumns);
        return builder.toString();
    }

    @Override
    protected String sanitizedUrl(final String url) {
        // MySQL can also have "username:password@" at the beginning of the host list and
        // in parenthetical properties
        return super.sanitizedUrl(url)
            .replaceAll("(?i)([(,]password=)[^,)]*", "$1****")
            .replaceAll("(://[^:]*:)([^@]*)@", "$1****@");
    }
}
