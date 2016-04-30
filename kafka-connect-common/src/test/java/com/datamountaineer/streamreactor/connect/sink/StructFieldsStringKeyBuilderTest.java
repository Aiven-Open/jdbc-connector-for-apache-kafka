package com.datamountaineer.streamreactor.connect.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StructFieldsStringKeyBuilderTest {
    @Test(expected = IllegalArgumentException.class)
    public void raiseExceptionIfFieldIsNotPresentInStruct() {

        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstName", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

        Struct struct = new Struct(schema).put("firstName", "Alex").put("age", 30);

        SinkRecord sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1);
        new StructFieldsStringKeyBuilder(new String[]{"threshold"}).build(sinkRecord);

    }

    @Test
    public void createTheKeyBasedOnASingleFieldInTheStruct() {
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstName", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

        Struct struct = new Struct(schema).put("firstName", "Alex").put("age", 30);

        SinkRecord sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1);
        assertEquals(new StructFieldsStringKeyBuilder(new String[]{"firstName"}).build(sinkRecord), "Alex");

    }

    @Test
    public void createTheKeyFromMultipleFieldsInTheStruct() {

        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstName", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

        Struct struct = new Struct(schema).put("firstName", "Alex").put("age", 30);

        SinkRecord sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1);
        assertEquals(new StructFieldsStringKeyBuilder(new String[]{"firstName", "age"}).build(sinkRecord), "Alex.30");
    }
}
