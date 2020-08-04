/*
 * Copyright 2019 Aiven Oy
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.IdentifierRules;
import io.aiven.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for SQL Server.
 */
public class SqlServerDatabaseDialect extends GenericDatabaseDialect {
    /**
     * The provider for {@link SqlServerDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(SqlServerDatabaseDialect.class.getSimpleName(),
                    "microsoft:sqlserver",
                    "sqlserver",
                    "jdbc:sqlserver",
                    "jtds:sqlserver");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new SqlServerDatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public SqlServerDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "[", "]"));
    }

    @Override
    protected boolean useCatalog() {
        // SQL Server uses JDBC's catalog to represent the database,
        // and JDBC's schema to represent the owner (e.g., "dbo")
        return true;
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
                    return "datetime2";
                default:
                    // pass through to normal types
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "tinyint";
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
                return "bit";
            case STRING:
                //900 is a max size for a column on which SQL server builds an index
                //here is the docs: https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms191241(v=sql.105)?redirectedfrom=MSDN
                return field.isPrimaryKey() ? "varchar(900)" : "varchar(max)";
            case BYTES:
                return "varbinary(max)";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildDropTableStatement(
        final TableId table,
        final DropOptions options
    ) {
        final ExpressionBuilder builder = expressionBuilder();

        if (options.ifExists()) {
            builder.append("IF OBJECT_ID('");
            builder.append(table);
            builder.append(", 'U') IS NOT NULL");
        }
        // SQL Server 2016 supports IF EXISTS
        builder.append("DROP TABLE ");
        builder.append(table);
        if (options.cascade()) {
            builder.append(" CASCADE");
        }
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

    @Override
    protected ColumnDefinition columnDefinition(
        final ResultSet resultSet,
        final ColumnId id,
        final int jdbcType,
        final String typeName,
        final String classNameForType,
        final ColumnDefinition.Nullability nullability,
        final ColumnDefinition.Mutability mutability,
        final int precision,
        final int scale,
        final Boolean signedNumbers,
        final Integer displaySize,
        Boolean autoIncremented,
        final Boolean caseSensitive,
        final Boolean searchable,
        final Boolean currency,
        final Boolean isPrimaryKey
    ) {
        try {
            final String isAutoIncremented = resultSet.getString(22);

            if ("yes".equalsIgnoreCase(isAutoIncremented)) {
                autoIncremented = Boolean.TRUE;
            } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
                autoIncremented = Boolean.FALSE;
            }
        } catch (final SQLException e) {
            log.warn("Unable to get auto incrementing column information", e);
        }

        return super.columnDefinition(
            resultSet,
            id,
            jdbcType,
            typeName,
            classNameForType,
            nullability,
            mutability,
            precision,
            scale,
            signedNumbers,
            displaySize,
            autoIncremented,
            caseSensitive,
            searchable,
            currency,
            isPrimaryKey
        );
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

    @Override
    protected String sanitizedUrl(final String url) {
        // SQL Server has semicolon delimited property name-value pairs, and several properties
        // that contain secrets
        return super.sanitizedUrl(url)
            .replaceAll("(?i)(;password=)[^;]*", "$1****")
            .replaceAll("(?i)(;keyStoreSecret=)[^;]*", "$1****")
            .replaceAll("(?i)(;gsscredential=)[^;]*", "$1****");
    }
}
