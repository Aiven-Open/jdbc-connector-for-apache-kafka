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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
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
 * A {@link DatabaseDialect} for SQL Server.
 */
public class SybaseDatabaseDialect extends GenericDatabaseDialect {
    /**
     * The provider for {@link SybaseDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(SybaseDatabaseDialect.class.getSimpleName(), "microsoft:sqlserver", "sqlserver",
                "jtds:sybase"
            );
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new SybaseDatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public SybaseDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected boolean useCatalog() {
        // Sybase uses JDBC's catalog to represent the database,
        // and JDBC's schema to represent the owner (e.g., "dbo")
        return true;
    }

    @Override
    protected String currentTimestampDatabaseQuery() {
        return "select getdate()";
    }

    @Override
    protected String checkConnectionQuery() {
        return "SELECT 1";
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "decimal(38," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
                case Date.LOGICAL_NAME:
                    return "date";
                case Time.LOGICAL_NAME:
                    return "time";
                case Timestamp.LOGICAL_NAME:
                    return "datetime";
                default:
                    // pass through to normal types
            }
        }
        switch (field.schemaType()) {
            case INT8:
                // 'tinyint' has a range of 0-255 and cannot handle negative numbers
                return "smallint";
            case INT16:
                return "smallint";
            case INT32:
                return "int";
            case INT64:
                return "bigint";
            case FLOAT32:
                return "real";
            case FLOAT64:
                return "float";
            case BOOLEAN:
                if (field.isOptional()) {
                    return "tinyint"; // Sybase does not allow 'bit' to be nullable
                } else {
                    return "bit";
                }
            case STRING:
                if (field.isPrimaryKey()) {
                    // Could always use 'text', except columns of type 'text', 'image' and 'unitext'
                    // cannot be used in indexes. Also, 2600 is the max allowable size of an index,
                    // so use something smaller if multiple columns are to be used in the index.
                    return "varchar(512)";
                } else {
                    return "text";
                }
            case BYTES:
                return "image";
            default:
                return super.getSqlType(field);
        }
    }

    protected boolean maybeBindPrimitive(
        final PreparedStatement statement,
        final int index,
        final Schema schema,
        final Object value
    ) throws SQLException {
        // First handle non-standard bindings ...
        switch (schema.type()) {
            case INT8:
                if (value instanceof Number) {
                    statement.setShort(index, ((Number) value).shortValue());
                    return true;
                }
                break;
            default:
                break;
        }
        return super.maybeBindPrimitive(statement, index, schema, value);
    }

    @Override
    public void applyDdlStatements(
        final Connection connection,
        final List<String> statements
    ) throws SQLException {
        final boolean autoCommit = connection.getAutoCommit();
        if (!autoCommit) {
            connection.setAutoCommit(true);
        }
        try (final Statement statement = connection.createStatement()) {
            for (final String ddlStatement : statements) {
                statement.executeUpdate(ddlStatement);
            }
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    @Override
    protected Set<ColumnId> primaryKeyColumns(
        final Connection connection,
        final String catalogPattern,
        final String schemaPattern,
        final String tablePattern
    ) throws SQLException {
        // Must be done only with auto-commit enabled?!
        final boolean autoCommit = connection.getAutoCommit();
        try {
            if (!autoCommit) {
                connection.setAutoCommit(true);
            }
            return super.primaryKeyColumns(connection, catalogPattern, schemaPattern, tablePattern);
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    @Override
    public String buildDropTableStatement(
        final TableId table,
        final DropOptions options
    ) {
        final ExpressionBuilder builder = expressionBuilder();

        if (options.ifExists()) {
            builder.append("IF EXISTS (");

            builder.append("SELECT 1 FROM sysobjects ");
            if (table.schemaName() != null) {
                builder.append("INNER JOIN sysusers ON sysobjects.uid=sysusers.uid ");
                builder.append("WHERE sysusers.name='");
                builder.append(table.schemaName());
                builder.append("' AND sysobjects.name='");
                builder.append(table.tableName());
            } else {
                builder.append("WHERE name='");
                builder.append(table.tableName());
            }
            builder.append("' AND type='U') ");
        }
        builder.append("DROP TABLE ");
        builder.append(table);

        // ASE 12 does not support cascade, and doing this is complex
        //    if (options.cascade()) {
        //      builder.append(" CASCADE");
        //    }
        return builder.toString();
    }

    @Override
    public List<String> buildAlterTable(
        final TableId table,
        final Collection<SinkRecordField> fields
    ) {
        final ExpressionBuilder builder = expressionBuilder();
        builder.append("ALTER TABLE ");
        builder.append(table);
        builder.append(" ADD");
        writeColumnsSpec(builder, fields);
        return Collections.singletonList(builder.toString());
    }

    @Override
    public String buildUpsertQueryStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        final ExpressionBuilder builder = expressionBuilder();
        builder.append("merge into ");
        builder.append(table);
        builder.append(" with (HOLDLOCK) AS target using (select ");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? AS "))
            .of(keyColumns, nonKeyColumns);
        builder.append(") AS incoming on (");
        builder.appendList()
            .delimitedBy(" and ")
            .transformedBy(this::transformAs)
            .of(keyColumns);
        builder.append(")");
        if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
            builder.append(" when matched then update set ");
            builder.appendList()
                .delimitedBy(",")
                .transformedBy(this::transformUpdate)
                .of(nonKeyColumns);
        }
        builder.append(" when not matched then insert (");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(nonKeyColumns, keyColumns);
        builder.append(") values (");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
            .of(nonKeyColumns, keyColumns);
        builder.append(");");
        return builder.toString();
    }

    private void transformAs(final ExpressionBuilder builder, final ColumnId col) {
        builder.append("target.")
            .appendIdentifier(col.name())
            .append("=incoming.")
            .appendIdentifier(col.name());
    }

    private void transformUpdate(final ExpressionBuilder builder, final ColumnId col) {
        builder.appendIdentifier(col.name())
            .append("=incoming.")
            .appendIdentifier(col.name());
    }
}
