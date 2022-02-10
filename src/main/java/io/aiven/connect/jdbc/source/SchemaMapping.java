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

package io.aiven.connect.jdbc.source;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;

/**
 * A mapping from a result set into a {@link Schema}. This mapping contains an array of {@link
 * FieldSetter} functions (one for each column in the result set), and the caller should iterate
 * over these and call the function with the result set.
 *
 * <p>This mapping contains the {@link DatabaseDialect.ColumnConverter} functions that should
 * be called for each row in the result set. and these are exposed to users of this class
 * via the {@link FieldSetter} function.
 */
public final class SchemaMapping {

    /**
     * Convert the result set into a {@link Schema}.
     *
     * @param schemaName the name of the schema; may be null
     * @param metadata   the result set metadata; never null
     * @param dialect    the dialect for the source database; never null
     * @return the schema mapping; never null
     * @throws SQLException if there is a problem accessing the result set metadata
     */
    public static SchemaMapping create(
        final String schemaName,
        final ResultSetMetaData metadata,
        final DatabaseDialect dialect
    ) throws SQLException {
        final Map<ColumnId, ColumnDefinition> colDefns = dialect.describeColumns(metadata);
        final Map<String, DatabaseDialect.ColumnConverter> colConvertersByFieldName = new LinkedHashMap<>();
        final SchemaBuilder builder = SchemaBuilder.struct().name(schemaName);
        int columnNumber = 0;
        for (final ColumnDefinition colDefn : colDefns.values()) {
            ++columnNumber;
            final String fieldName = dialect.addFieldToSchema(colDefn, builder);
            if (fieldName == null) {
                continue;
            }
            final Field field = builder.field(fieldName);
            final ColumnMapping mapping = new ColumnMapping(colDefn, columnNumber, field);
            final DatabaseDialect.ColumnConverter converter = dialect.createColumnConverter(mapping);
            colConvertersByFieldName.put(fieldName, converter);
        }
        final Schema schema = builder.build();
        return new SchemaMapping(schema, colConvertersByFieldName);
    }

    private final Schema schema;
    private final List<FieldSetter> fieldSetters;

    private SchemaMapping(
        final Schema schema,
        final Map<String, DatabaseDialect.ColumnConverter> convertersByFieldName
    ) {
        assert schema != null;
        assert convertersByFieldName != null;
        assert !convertersByFieldName.isEmpty();
        this.schema = schema;
        final List<FieldSetter> fieldSetters = new ArrayList<>(convertersByFieldName.size());
        for (final Map.Entry<String, DatabaseDialect.ColumnConverter> entry
            : convertersByFieldName.entrySet()) {
            final DatabaseDialect.ColumnConverter converter = entry.getValue();
            final Field field = schema.field(entry.getKey());
            assert field != null;
            fieldSetters.add(new FieldSetter(converter, field));
        }
        this.fieldSetters = Collections.unmodifiableList(fieldSetters);
    }

    public Schema schema() {
        return schema;
    }

    /**
     * Get the {@link FieldSetter} functions, which contain one for each result set column whose
     * values are to be mapped/converted and then set on the corresponding {@link Field} in supplied
     * {@link Struct} objects.
     *
     * @return the array of {@link FieldSetter} instances; never null and never empty
     */
    List<FieldSetter> fieldSetters() {
        return fieldSetters;
    }

    @Override
    public String toString() {
        return "Mapping for " + schema.name();
    }

    public static final class FieldSetter {

        private final DatabaseDialect.ColumnConverter converter;
        private final Field field;

        private FieldSetter(
            final DatabaseDialect.ColumnConverter converter,
            final Field field
        ) {
            this.converter = converter;
            this.field = field;
        }

        /**
         * Get the {@link Field} that this setter function sets.
         *
         * @return the field; never null
         */
        public Field field() {
            return field;
        }

        /**
         * Call the {@link DatabaseDialect.ColumnConverter converter} on the supplied {@link ResultSet}
         * and set the corresponding {@link #field() field} on the supplied {@link Struct}.
         *
         * @param struct    the struct whose field is to be set with the converted value from the result
         *                  set; may not be null
         * @param resultSet the result set positioned at the row to be processed; may not be null
         * @throws SQLException if there is an error accessing the result set
         * @throws IOException  if there is an error accessing a streaming value from the result set
         */
        void setField(
            final Struct struct,
            final ResultSet resultSet
        ) throws SQLException, IOException {
            final Object value = this.converter.convert(resultSet);
            if (resultSet.wasNull()) {
                struct.put(field, null);
            } else {
                struct.put(field, value);
            }
        }

        @Override
        public String toString() {
            return field.name();
        }
    }
}
