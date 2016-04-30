package com.datamountaineer.streamreactor.connect;

import com.datamountaineer.streamreactor.connect.sink.SinkRecordKeyStringKeyBuilder;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;


public class StringGenericRowKeyBuilderTest {
    private final SinkRecordKeyStringKeyBuilder keyRowKeyBuilder = new SinkRecordKeyStringKeyBuilder();

    @Test
    public void createTheKeyFromSchemaKeyValueWhenItIsAByte() {
        byte b = 123;

        final SinkRecord sinkRecord = new SinkRecord("", 1, Schema.INT8_SCHEMA, b, Schema.FLOAT64_SCHEMA, null, 0);

        assertEquals(keyRowKeyBuilder.build(sinkRecord), "123");

    }

    @Test
    public void createTheKeyFromSchemaKeyValueWhenItIsAString() {
        final String s = "somekey";
        final SinkRecord sinkRecord = new SinkRecord("", 1, Schema.STRING_SCHEMA, s, Schema.FLOAT64_SCHEMA, null, 0);

        assertEquals(keyRowKeyBuilder.build(sinkRecord), s);
    }

    @Test
    public void createTheKeyFromSchemaKeyValueWhenItIsBytes() {

        final byte[] bArray = {23, 24, -42};
        final SinkRecord sinkRecord = new SinkRecord("", 1, Schema.BYTES_SCHEMA, bArray, Schema.FLOAT64_SCHEMA, null, 0);
        assertEquals(keyRowKeyBuilder.build(sinkRecord), Joiner.on(".").join(Lists.newArrayList(bArray)));
    }

    @Test
    public void createTheKeyFromSchemaKeyValueWhenItIsBoolean() {
        final boolean bool = true;
        final SinkRecord sinkRecord = new SinkRecord("", 1, Schema.BOOLEAN_SCHEMA, bool, Schema.FLOAT64_SCHEMA, null, 0);

        assertEquals(keyRowKeyBuilder.build(sinkRecord), "true");

    }
}
