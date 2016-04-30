package com.datamountaineer.streamreactor.connect.sink;

import com.google.common.base.Joiner;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringGenericRowKeyBuilderTest {
    @Test
    public void createTheKeyFromSinkRow() {

        String topic = "sometopic";
        int partition = 2;
        long offset = 1243L;
        SinkRecord sinkRecord = new SinkRecord(topic, partition, Schema.INT32_SCHEMA, 345, Schema.STRING_SCHEMA, "", offset);

        StringGenericRowKeyBuilder keyBuilder = new StringGenericRowKeyBuilder(".");
        String expected = Joiner.on(".").join(topic, partition, offset);
        assertEquals(keyBuilder.build(sinkRecord), expected);
    }
}
