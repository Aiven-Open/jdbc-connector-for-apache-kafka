/*
 * Copyright 2020 Aiven Oy
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
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.source.ColumnMapping;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.IdentifierRules;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for PostgreSQL.
 */
public class PostgreSqlDatabaseDialect extends GenericDatabaseDialect {

    /**
     * The provider for {@link PostgreSqlDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(PostgreSqlDatabaseDialect.class.getSimpleName(), "postgresql");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new PostgreSqlDatabaseDialect(config);
        }
    }

    protected static final String JSON_TYPE_NAME = "json";

    protected static final String JSONB_TYPE_NAME = "jsonb";

    protected static final String UUID_TYPE_NAME = "uuid";

    private static final List<String> CAST_TYPES = List.of(JSON_TYPE_NAME, JSONB_TYPE_NAME, UUID_TYPE_NAME);

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public PostgreSqlDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
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
    public String addFieldToSchema(
            final ColumnDefinition columnDefn,
            final SchemaBuilder builder
    ) {
        // Add the PostgreSQL-specific types first
        final String fieldName = fieldNameFor(columnDefn);
        switch (columnDefn.type()) {
            case Types.BIT: {
                // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
                // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
                // this as well as lengths larger than 8.
                final boolean optional = columnDefn.isOptional();
                final int numBits = columnDefn.precision();
                final Schema schema;
                if (numBits <= 1) {
                    schema = optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
                } else if (numBits <= 8) {
                    // For consistency with what the connector did before ...
                    schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
                } else {
                    schema = optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
                }
                builder.field(fieldName, schema);
                return fieldName;
            }
            case Types.OTHER: {
                // Some of these types will have fixed size, but we drop this from the schema conversion
                // since only fixed byte arrays can have a fixed size
                if (isJsonType(columnDefn)) {
                    builder.field(
                            fieldName,
                            columnDefn.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA
                    );
                    return fieldName;
                }
                if (isUuidType(columnDefn)) {
                    builder.field(
                            fieldName,
                            columnDefn.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA
                    );
                    return fieldName;
                }
                break;
            }
            default:
                break;
        }

        // Delegate for the remaining logic
        return super.addFieldToSchema(columnDefn, builder);
    }

    @Override
    public ColumnConverter createColumnConverter(
            final ColumnMapping mapping
    ) {
        // First handle any PostgreSQL-specific types
        final ColumnDefinition columnDefn = mapping.columnDefn();
        final int col = mapping.columnNumber();
        switch (columnDefn.type()) {
            case Types.BIT: {
                // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
                // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
                // this as well as lengths larger than 8.
                final int numBits = columnDefn.precision();
                if (numBits <= 1) {
                    return rs -> rs.getBoolean(col);
                } else if (numBits <= 8) {
                    // Do this for consistency with earlier versions of the connector
                    return rs -> rs.getByte(col);
                }
                return rs -> rs.getBytes(col);
            }
            case Types.OTHER: {
                if (isJsonType(columnDefn)) {
                    return rs -> rs.getString(col);
                }
                if (isUuidType(columnDefn)) {
                    return rs -> rs.getString(col);
                }
                break;
            }
            default:
                break;
        }

        // Delegate for the remaining logic
        return super.createColumnConverter(mapping);
    }

    protected boolean isJsonType(final ColumnDefinition columnDefn) {
        final String typeName = columnDefn.typeName();
        return JSON_TYPE_NAME.equalsIgnoreCase(typeName) || JSONB_TYPE_NAME.equalsIgnoreCase(typeName);
    }

    protected boolean isUuidType(final ColumnDefinition columnDefn) {
        return UUID.class.getName().equals(columnDefn.classNameForType());
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // fall through to normal types
            }
        }
        switch (field.schemaType()) {
            case INT8:
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "TEXT";
            case BYTES:
                return "BYTEA";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildInsertStatement(final TableId table,
                                       final TableDefinition tableDefinition,
                                       final Collection<ColumnId> keyColumns,
                                       final Collection<ColumnId> nonKeyColumns) {
        return expressionBuilder()
                .append("INSERT INTO ")
                .append(table)
                .append("(")
                .appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns)
                .append(") VALUES(")
                .appendList()
                .delimitedBy(",")
                .transformedBy(transformColumn(tableDefinition))
                .of(keyColumns, nonKeyColumns)
                .append(")")
                .toString();
    }

    @Override
    public String buildUpdateStatement(final TableId table,
                                       final TableDefinition tableDefinition,
                                       final Collection<ColumnId> keyColumns,
                                       final Collection<ColumnId> nonKeyColumns) {
        final ExpressionBuilder.Transform<ColumnId> columnTransform = (builder, columnId) -> {
            builder.append(columnId.name())
                    .append("=?")
                    .append(cast(tableDefinition, columnId));
        };

        final ExpressionBuilder builder = expressionBuilder();
        builder.append("UPDATE ")
                .append(table)
                .append(" SET ")
                .appendList()
                .delimitedBy(",")
                .transformedBy(columnTransform)
                .of(nonKeyColumns);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                    .delimitedBy(" AND ")
                    .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                    .of(keyColumns);
        }
        return builder.toString();
    }

    @Override
    public String buildUpsertQueryStatement(final TableId table,
                                            final TableDefinition tableDefinition,
                                            final Collection<ColumnId> keyColumns,
                                            final Collection<ColumnId> nonKeyColumns) {
        final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
            builder.appendIdentifier(col.name())
                    .append("=EXCLUDED.")
                    .appendIdentifier(col.name());
        };

        final ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ")
                .append(table)
                .append(" (")
                .appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns)
                .append(") VALUES (")
                .appendList()
                .delimitedBy(",")
                .transformedBy(transformColumn(tableDefinition))
                .of(keyColumns, nonKeyColumns)
                .append(") ON CONFLICT (")
                .appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns);
        builder.append(") ");
        if (nonKeyColumns.isEmpty()) {
            builder.append("DO NOTHING");
        } else {
            builder.append("DO UPDATE SET ");
            builder.appendList()
                    .delimitedBy(",")
                    .transformedBy(transform)
                    .of(nonKeyColumns);
        }
        return builder.toString();
    }

    private ExpressionBuilder.Transform<ColumnId> transformColumn(final TableDefinition tableDefinition) {
        return (builder, column) -> {
            builder.append("?");
            builder.append(cast(tableDefinition, column));
        };
    }

    private String cast(final TableDefinition tableDfn, final ColumnId columnId) {
        if (Objects.nonNull(tableDfn)) {
            final var columnDef = tableDfn.definitionForColumn(columnId.name());
            final var typeName = columnDef.typeName();
            if (Objects.nonNull(typeName)) {
                if (CAST_TYPES.contains(typeName.toLowerCase())) {
                    return "::" + typeName.toLowerCase();
                }
            }
        }
        return "";
    }

}
