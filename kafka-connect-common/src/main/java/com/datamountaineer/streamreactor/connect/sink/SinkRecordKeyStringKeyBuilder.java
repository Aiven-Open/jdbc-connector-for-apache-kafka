package com.datamountaineer.streamreactor.connect.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;


/**
 * Creates a key based on the connect SinkRecord instance key. Only connect Schema primitive types are handled
 */
public final class SinkRecordKeyStringKeyBuilder implements StringKeyBuilder {

    @Override
    public String build(SinkRecord record) {
        final Schema.Type keySchemaType = record.keySchema().type();
        assert (keySchemaType.isPrimitive()) : "The SinkRecord key schema is not a primitive type";

        switch (keySchemaType.name()) {
            case "INT8":
            case "INT16":
            case "INT32":
            case "INT64":
            case "FLOAT32":
            case "FLOAT64":
            case "BOOLEAN":
            case "STRING":
            case "BYTES":
                return record.key().toString();

            default:
                throw new IllegalArgumentException(keySchemaType.name() + " is not supported by the +" + getClass().getName());
        }

    }
}
