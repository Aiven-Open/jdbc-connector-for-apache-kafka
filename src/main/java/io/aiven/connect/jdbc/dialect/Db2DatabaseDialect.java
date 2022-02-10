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
 * A {@link DatabaseDialect} for IBM DB2.
 */
public class Db2DatabaseDialect extends GenericDatabaseDialect {

    /**
     * The provider for {@link Db2DatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(Db2DatabaseDialect.class.getSimpleName(), "db2", "db2j", "ibmdb");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new Db2DatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public Db2DatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected String currentTimestampDatabaseQuery() {
        return "SELECT CURRENT_TIMESTAMP(12) FROM SYSIBM.SYSDUMMY1";
    }

    @Override
    protected String checkConnectionQuery() {
        return "SELECT 1 FROM SYSIBM.SYSDUMMY1";
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL(31," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
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
                return "SMALLINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "SMALLINT";
            case STRING:
                return "VARCHAR(32672)";
            case BYTES:
                return "BLOB(64000)";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildUpsertQueryStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        // http://lpar.ath0.com/2013/08/12/upsert-in-db2/
        final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
            builder.append(table)
                .append(".")
                .appendIdentifier(col.name())
                .append("=DAT.")
                .appendIdentifier(col.name());
        };

        final ExpressionBuilder builder = expressionBuilder();
        builder.append("merge into ");
        builder.append(table);
        builder.append(" using (values(");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.placeholderInsteadOfColumnNames("?"))
            .of(keyColumns, nonKeyColumns);
        builder.append(")) as DAT(");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
        builder.append(") on ");
        builder.appendList()
            .delimitedBy(" and ")
            .transformedBy(transform)
            .of(keyColumns);
        if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
            builder.append(" when matched then update set ");
            builder.appendList()
                .delimitedBy(", ")
                .transformedBy(transform)
                .of(nonKeyColumns);
        }

        builder.append(" when not matched then insert(");
        builder.appendList().delimitedBy(",").of(nonKeyColumns, keyColumns);
        builder.append(") values(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("DAT."))
            .of(nonKeyColumns, keyColumns);
        builder.append(")");
        return builder.toString();
    }

    @Override
    protected String sanitizedUrl(final String url) {
        // DB2 has semicolon delimited property name-value pairs
        return super.sanitizedUrl(url)
            .replaceAll("(?i)([:;]password=)[^;]*", "$1****");
    }
}
