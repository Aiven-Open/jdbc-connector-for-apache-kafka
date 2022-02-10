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

import java.util.ArrayList;
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
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.IdentifierRules;
import io.aiven.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for SQLite.
 */
public class SqliteDatabaseDialect extends GenericDatabaseDialect {

    /**
     * The provider for {@link SqliteDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(SqliteDatabaseDialect.class.getSimpleName(), "sqlite");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new SqliteDatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public SqliteDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected boolean includeTable(final TableId table) {
        // SQLite JDBC driver does not correctly mark these as system tables
        return !table.tableName().startsWith("sqlite_");
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                case Timestamp.LOGICAL_NAME:
                    return "NUMERIC";
                default:
                    // pass through to normal types
            }
        }
        switch (field.schemaType()) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return "INTEGER";
            case FLOAT32:
            case FLOAT64:
                return "REAL";
            case STRING:
                return "TEXT";
            case BYTES:
                return "BLOB";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public List<String> buildAlterTable(
        final TableId table,
        final Collection<SinkRecordField> fields
    ) {
        final List<String> queries = new ArrayList<>(fields.size());
        for (final SinkRecordField field : fields) {
            queries.addAll(super.buildAlterTable(table, Collections.singleton(field)));
        }
        return queries;
    }

    @Override
    public String buildUpsertQueryStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        final ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT OR REPLACE INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }


    @Override
    protected String currentTimestampDatabaseQuery() {
        return "SELECT strftime('%Y-%m-%d %H:%M:%S.%f', 'now')";
    }
}
