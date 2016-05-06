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


package com.datamountaineer.streamreactor.connect;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class StructFieldsValuesExtractorImpl implements StructFieldsValuesExtractor {
    private final Boolean includeAllFields;
    private final Map<String, String> fieldsAliasMap;

    public StructFieldsValuesExtractorImpl(Boolean includeAllFields, Map<String, String> fieldsAliasMap) {

        this.includeAllFields = includeAllFields;
        this.fieldsAliasMap = fieldsAliasMap;
    }

    @Override
    public List<FieldNameAndValue> get(final Struct struct) {
        final Schema schema = struct.schema();

        List<Field> fields;
        if (includeAllFields) {
            fields = schema.fields();
        } else {
            fields = new ArrayList<>();
            for (final Field f : schema.fields()) {
                if (fieldsAliasMap.containsKey(f.name()))
                    fields.add(f);
            }
        }

        final List<FieldNameAndValue> result = new ArrayList<>();
        for (final Field f : fields) {
            final Object value = getFieldValue(f, struct);
            if (value != null) {
                String key = f.name();
                if (fieldsAliasMap.containsKey(key)) {
                    key = (String) fieldsAliasMap.get(key);
                }
                result.add(new FieldNameAndValue(key, value));
            }
        }
        return result;
    }

    private Object getFieldValue(final Field field, final Struct struct) {
        final Object value = struct.get(field);
        if (value == null)
            return null;

        final String fieldName = field.name();
        switch (field.schema().type()) {
            case BOOLEAN:
                return struct.getBoolean(fieldName);

            case BYTES:
                return struct.getBytes(fieldName);

            case FLOAT32:
                return struct.getFloat32(fieldName);

            case FLOAT64:
                return struct.getFloat64(fieldName);

            case INT8:
                return struct.getInt8(fieldName);

            case INT16:
                return struct.getInt16(fieldName);

            case INT32:
                return struct.getInt32(fieldName);

            case INT64:
                return struct.getInt64(fieldName);

            case STRING:
                return struct.getString(fieldName);

            default:
                throw new IllegalArgumentException(field.schema() + " is no supported. ");
        }
    }
}
 