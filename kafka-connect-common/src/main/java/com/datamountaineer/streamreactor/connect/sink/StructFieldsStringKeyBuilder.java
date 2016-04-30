package com.datamountaineer.streamreactor.connect.sink;


import com.datamountaineer.streamreactor.connect.util.StringUtils;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashSet;
import java.util.Set;

/**
 * Builds a new key from the payload fields specified
 */
final class StructFieldsStringKeyBuilder implements StringKeyBuilder {

    private final String keyDelimiter;
    private final String[] keys;

    private static final Set<Schema> availableSchemas = new HashSet<>(Lists.newArrayList(
            new Schema[]{
                    Schema.BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA,
                    Schema.BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA,
                    Schema.FLOAT32_SCHEMA, Schema.OPTIONAL_FLOAT32_SCHEMA,
                    Schema.FLOAT64_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA,
                    Schema.INT8_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA,
                    Schema.INT16_SCHEMA, Schema.OPTIONAL_INT16_SCHEMA,
                    Schema.INT32_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA,
                    Schema.INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA,
                    Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA}));


    /**
     * @param keys         - A collection of field names present in the incoming SinkRecord which make the entry key
     * @param keyDelimiter - The separator to use as it joins two strings.
     */
    public StructFieldsStringKeyBuilder(String[] keys, String keyDelimiter) {
        this.keys = keys;
        this.keyDelimiter = keyDelimiter;
    }

    public StructFieldsStringKeyBuilder(String[] keys) {
        this(keys, ".");
    }

    @Override
    public String build(SinkRecord record) {
        final Struct struct = (Struct) record.value();
        final Schema schema = struct.schema();

        final HashSet<String> givenKeys = new HashSet<>(Lists.newArrayList(keys));

        for (Field f : schema.fields()) {
            givenKeys.remove(f.name());
        }

        if (givenKeys.size() > 0) {
            final String missingKeys = StringUtils.join(givenKeys, ",");
            throw new IllegalArgumentException(missingKeys + " keys are not present in the SinkRecord payload");
        }

        final StringBuilder builder = new StringBuilder();

        for (String key : keys) {
            final Field field = schema.field(key);
            final Object value = struct.get(field);
            if (value == null) {
                throw new IllegalArgumentException(key + " field value is null. Non null value is required for the fileds creating the record key");
            }

            if (availableSchemas.contains(field.schema())) {
                if (builder.length() > 0)
                    builder.append(keyDelimiter);
                builder.append(value.toString());

            }
        }
        return builder.toString();
    }
}