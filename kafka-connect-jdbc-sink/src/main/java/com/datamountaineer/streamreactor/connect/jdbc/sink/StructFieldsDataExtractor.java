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

public class StructFieldsDataExtractor {
    private final boolean includeAllFields;
    private final Map<String, String> fieldsAliasMap;

    public StructFieldsDataExtractor(boolean includeAllFields, Map<String, String> fieldsAliasMap) {
        this.includeAllFields = includeAllFields;
        this.fieldsAliasMap = fieldsAliasMap;
    }

    public List<Pair<String, PreparedStatementBinder>> get(final Struct struct) {
        final Schema schema = struct.schema();
        final Collection<Field> fields;
        if (includeAllFields)
            fields = schema.fields();
        else {
            fields = Collections2.filter(schema.fields(), new Predicate<Field>() {
                @Override
                public boolean apply(Field input) {
                    return fieldsAliasMap.containsKey(input.name());
                }

                @Override
                public boolean equals(Object object) {
                    return false;
                }
            });
        }

        final List<Pair<String, PreparedStatementBinder>> binders = Lists.newArrayList();
        for (final Field field : fields) {
            final PreparedStatementBinder binder = getFieldValue(field, struct);
            if (binder != null) {
                binders.add(new Pair(field.name(), binder));
            }
        }

        return binders;
    }

    private PreparedStatementBinder getFieldValue(final Field field, final Struct struct) {
        final Object value = struct.get(field);
        if (value == null)
            return null;


        final String fieldName = field.name();

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
                throw new IllegalArgumentException("Following schmea type " + struct.schema().type() + " is not supported");
        }
        return binder;
    }
}

