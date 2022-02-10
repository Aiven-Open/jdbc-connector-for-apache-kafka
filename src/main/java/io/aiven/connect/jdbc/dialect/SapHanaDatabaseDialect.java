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
 * A {@link DatabaseDialect} for SAP.
 */
public class SapHanaDatabaseDialect extends GenericDatabaseDialect {
    /**
     * The provider for {@link SapHanaDatabaseDialect}.
     *
     * <p>HANA url's are in the format: {@code jdbc:sap://$host:3(instance)(port)/}
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(SapHanaDatabaseDialect.class.getSimpleName(), "sap");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new SapHanaDatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public SapHanaDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected String checkConnectionQuery() {
        return "SELECT DATABASE_NAME FROM SYS.M_DATABASES";
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
                    return "DATE";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // fall through to normal types
                    break;
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(1000)";
            case BYTES:
                return "BLOB";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildCreateTableStatement(
        final TableId table,
        final Collection<SinkRecordField> fields
    ) {
        // Defaulting to Column Store
        return super.buildCreateTableStatement(table, fields)
            .replace("CREATE TABLE", "CREATE COLUMN TABLE");
    }

    @Override
    public List<String> buildAlterTable(
        final TableId table,
        final Collection<SinkRecordField> fields
    ) {
        final ExpressionBuilder builder = expressionBuilder();
        builder.append("ALTER TABLE ");
        builder.append(table);
        builder.append(" ADD(");
        writeColumnsSpec(builder, fields);
        builder.append(")");
        return Collections.singletonList(builder.toString());
    }

    @Override
    public String buildUpsertQueryStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        // https://help.sap.com/hana_one/html/sql_replace_upsert.html
        final ExpressionBuilder builder = expressionBuilder();
        builder.append("UPSERT ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        builder.append(" WITH PRIMARY KEY");
        return builder.toString();
    }
}
