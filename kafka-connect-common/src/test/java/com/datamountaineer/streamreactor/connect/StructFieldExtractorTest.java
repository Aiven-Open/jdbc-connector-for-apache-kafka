package com.datamountaineer.streamreactor.connect;

import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StructFieldExtractorTest {

    @Test
    public void returnAllFieldsAndTheirBytesValue() {
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstName", Schema.STRING_SCHEMA)
                .field("lastName", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

        Struct struct = new Struct(schema)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("age", 30);

        Map<String, Object> map = FieldNameAndValue.
                toMap(new StructFieldsValuesExtractorImpl(true, new HashMap<String, String>()).get(struct));


        assertEquals((String) map.get("firstName"), "Alex");
        assertEquals((String) map.get("lastName"), "Smith");
        assertEquals((int) map.get("age"), 30);
    }

    @Test
    public void returnAllFieldsAndApplyMappings() {


        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstName", Schema.STRING_SCHEMA)
                .field("lastName", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

        Struct struct = new Struct(schema)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("age", 30);

        Map<String, String> mappings = Maps.newHashMap();
        mappings.put("lastName", "Name");
        mappings.put("age", "a");
        Map<String, Object> map = FieldNameAndValue.toMap(new StructFieldsValuesExtractorImpl(true, mappings).get(struct));

        assertEquals((String) map.get("firstName"), "Alex");
        assertEquals((String) map.get("Name"), "Smith");
        assertEquals((int) map.get("a"), 30);

    }

    @Test
    public void returnOnlyTheSpecifiedFields() {

        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstName", Schema.STRING_SCHEMA)
                .field("lastName", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

        Struct struct = new Struct(schema)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("age", 30);

        Map<String, String> mappings = Maps.newHashMap();
        mappings.put("lastName", "Name");
        mappings.put("age", "age");
        Map<String, Object> map = FieldNameAndValue.toMap(new StructFieldsValuesExtractorImpl(false, mappings).get(struct));

        assertEquals(map.get("Name"), "Smith");
        assertEquals((int) map.get("age"), 30);

        assertEquals(map.size(), 2);
    }

}
