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
 * A {@link DatabaseDialect} for Oracle.
 */
public class OracleDatabaseDialect extends GenericDatabaseDialect {

    /**
     * The provider for {@link OracleDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(OracleDatabaseDialect.class.getSimpleName(), "oracle");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new OracleDatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public OracleDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected String currentTimestampDatabaseQuery() {
        return "select CURRENT_TIMESTAMP from dual";
    }

    @Override
    protected String checkConnectionQuery() {
        return "SELECT 1 FROM DUAL";
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "NUMBER(*," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "DATE";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // fall through to normal types
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "NUMBER(3,0)";
            case INT16:
                return "NUMBER(5,0)";
            case INT32:
                return "NUMBER(10,0)";
            case INT64:
                return "NUMBER(19,0)";
            case FLOAT32:
                return "BINARY_FLOAT";
            case FLOAT64:
                return "BINARY_DOUBLE";
            case BOOLEAN:
                return "NUMBER(1,0)";
            case STRING:
                return "CLOB";
            case BYTES:
                return "BLOB";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildDropTableStatement(
        final TableId table,
        final DropOptions options
    ) {
        // https://stackoverflow.com/questions/1799128/oracle-if-table-exists
        ExpressionBuilder builder = expressionBuilder();

        builder.append("DROP TABLE ");
        builder.append(table);
        if (options.cascade()) {
            builder.append(" CASCADE CONSTRAINTS");
        }
        final String dropStatement = builder.toString();

        if (!options.ifExists()) {
            return dropStatement;
        }
        builder = expressionBuilder();
        builder.append("BEGIN ");
        // The drop statement includes double quotes for identifiers, so that's compatible with the
        // single quote used to delimit the string literal
        // https://docs.oracle.com/cd/B28359_01/appdev.111/b28370/literal.htm#LNPLS01326
        builder.append("EXECUTE IMMEDIATE '" + dropStatement + "' ");
        builder.append("EXCEPTION ");
        builder.append("WHEN OTHERS THEN ");
        builder.append("IF SQLCODE != -942 THEN ");
        builder.append("    RAISE;");
        builder.append("END IF;");
        builder.append("END;");
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
        // https://blogs.oracle.com/cmar/entry/using_merge_to_do_an
        final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
            builder.append(table)
                .append(".")
                .appendIdentifier(col.name())
                .append("=incoming.")
                .appendIdentifier(col.name());
        };

        final ExpressionBuilder builder = expressionBuilder();
        builder.append("merge into ");
        builder.append(table);
        builder.append(" using (select ");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? "))
            .of(keyColumns, nonKeyColumns);
        builder.append(" FROM dual) incoming on(");
        builder.appendList()
            .delimitedBy(" and ")
            .transformedBy(transform)
            .of(keyColumns);
        builder.append(")");
        if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
            builder.append(" when matched then update set ");
            builder.appendList()
                .delimitedBy(",")
                .transformedBy(transform)
                .of(nonKeyColumns);
        }

        builder.append(" when not matched then insert(");
        builder.appendList()
            .delimitedBy(",")
            .of(nonKeyColumns, keyColumns);
        builder.append(") values(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
            .of(nonKeyColumns, keyColumns);
        builder.append(")");
        return builder.toString();
    }

    @Override
    protected String sanitizedUrl(final String url) {
        // Oracle can also have ":username/password@" after the driver type
        return super.sanitizedUrl(url)
            .replaceAll("(:thin:[^/]*)/([^@]*)@", "$1/****@")
            .replaceAll("(:oci[^:]*:[^/]*)/([^@]*)@", "$1/****@");
    }
}
