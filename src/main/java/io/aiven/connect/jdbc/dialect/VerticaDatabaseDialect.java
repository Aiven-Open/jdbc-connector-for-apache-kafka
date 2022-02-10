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
import io.aiven.connect.jdbc.util.IdentifierRules;
import io.aiven.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for Vertica.
 */
public class VerticaDatabaseDialect extends GenericDatabaseDialect {
    /**
     * The provider for {@link VerticaDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(VerticaDatabaseDialect.class.getSimpleName(), "vertica");
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new VerticaDatabaseDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public VerticaDatabaseDialect(final JdbcConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL(18," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // fall through to non-logical types
                    break;
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "INT";
            case INT16:
                return "INT";
            case INT32:
                return "INT";
            case INT64:
                return "INT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "FLOAT";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(1024)";
            case BYTES:
                return "VARBINARY(1024)";
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
}
