/**
 * Copyright 2015 Datamountaineer.
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
 **/


package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.Pair;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.*;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This class holds the a mappings of fields to extract from
 * a Connect Struct record.
 *
 * Used to building mappings fro Struct records to JDBC binding statements.
 *
 * For example, if the struct contains a string field which is part of the aliasMap .i.e.
 * set in configuration to be write to the target, it will return a StringPreparedStatementBinder
 * (statement.setString(index, value))
 * */
public class StructFieldsDataExtractor {
    private final boolean includeAllFields;
    private final Map<String, String> fieldsAliasMap;

    public StructFieldsDataExtractor(boolean includeAllFields, Map<String, String> fieldsAliasMap) {
        this.includeAllFields = includeAllFields;
        this.fieldsAliasMap = fieldsAliasMap;
    }

    /**
     * For a Struct record build list of PreparedStatementBinders based on the structs fields.
     *
     * @param struct A Connect Struct record to extract fields from
     * @return A list if mappings of field name to PreparedStatementBinder for each field in the struct.
     * */
    public List<Pair<String, PreparedStatementBinder>> get(final Struct struct) {
        //get the records schema
        final Schema schema = struct.schema();
        final Collection<Field> fields;

        //if all fields take all the fields from the schema else filter out the fields based on the provided map
        if (includeAllFields)
            fields = schema.fields();
        else {
            fields = Collections2.filter(schema.fields(), new Predicate<Field>() {
                @Override
                public boolean apply(Field input) {
                    //filter for field name in map
                    return fieldsAliasMap.containsKey(input.name()) || includeAllFields;
                }
                @Override
                public boolean equals(Object object) {
                    return false;
                }
            });
        }

        //For each field convert the Connect schema type and extract the value, add to list and return
        final List<Pair<String, PreparedStatementBinder>> binders = Lists.newArrayList();
        for (final Field field : fields) {
            //get the correct binder based on the struct fields type
            final PreparedStatementBinder binder = getFieldValue(field, struct);
            if (binder != null) {
                String fieldName;
                if (fieldsAliasMap.containsKey(field.name())) {
                    fieldName = fieldsAliasMap.get(field.name());
                }
                else {
                    fieldName = field.name();
                }
                binders.add(new Pair(fieldName, binder));
            }
        }
        return binders;
    }

    /**
     * Return a PreparedStatementBinder for a struct fields.
     *
     * @param field The struct field to get the binder for.
     * @param struct The struct which the field belongs to.
     * @return A PreparedStatementBinder for the field.
     * */
    private PreparedStatementBinder getFieldValue(final Field field, final Struct struct) {
        final Object value = struct.get(field);
        if (value == null) {
            return null;
        }

        final String fieldName = field.name();

        //match on fields schema type to find the correct casting.
        PreparedStatementBinder binder;
        switch (field.schema().type()) {
            case INT8:
                binder = new BytePreparedStatementBinder(struct.getInt8(fieldName));
                break;
            case INT16:
                binder = new ShortPreparedStatementBinder(struct.getInt16(fieldName));
                break;
            case INT32:
                binder = new IntPreparedStatementBinder(struct.getInt32(fieldName));
                break;
            case INT64:
                binder = new LongPreparedStatementBinder(struct.getInt64(fieldName));
                break;
            case FLOAT32:
                binder = new FloatPreparedStatementBinder(struct.getFloat32(fieldName));
                break;
            case FLOAT64:
                binder = new DoublePreparedStatementBinder(struct.getFloat64(fieldName));
                break;
            case BOOLEAN:
                binder = new BooleanPreparedStatementBinder(struct.getBoolean(fieldName));
                break;
            case STRING:
                binder = new StringPreparedStatementBinder(struct.getString(fieldName));
                break;
            case BYTES:
                binder = new BytesPreparedStatementBinder(struct.getBytes(fieldName));
                break;
            default:
                throw new IllegalArgumentException("Following schema type " + struct.schema().type() + " is not supported");
        }
        return binder;
    }
}

