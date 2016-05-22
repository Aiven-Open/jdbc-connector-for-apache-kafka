package com.datamountaineer.streamreactor.connect.jdbc.sink;


import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BytePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BytesPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.DoublePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.FloatPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.IntPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.LongPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.ShortPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.StringPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StructFieldsDataExtractorTest {
  @Test
  public void returnAllFieldsAndTheirBytesValue() {
    Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("bool", Schema.BOOLEAN_SCHEMA)
            .field("short", Schema.INT16_SCHEMA)
            .field("byte", Schema.INT8_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

    short s = 1234;
    byte b = -32;
    long l = 12425436;
    float f = (float) 2356.3;
    double d = -2436546.56457;
    byte[] bs = new byte[]{-32, 124};

    Struct struct = new Struct(schema)
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("bool", true)
            .put("short", s)
            .put("byte", b)
            .put("long", l)
            .put("float", f)
            .put("double", d)
            .put("bytes", bs)
            .put("age", 30);

    FieldsMappings tm = new FieldsMappings("table", "topic", true, new HashMap<String, FieldAlias>());
    StructFieldsDataExtractor dataExtractor = new StructFieldsDataExtractor(tm);
    StructFieldsDataExtractor.PreparedStatementBinders binders = dataExtractor.get(struct,
            new SinkRecord("", 1, null, null, schema, struct, 0));

    HashMap<String, PreparedStatementBinder> map = new HashMap<>();
    for (PreparedStatementBinder p : Iterables.concat(binders.getNonKeyColumns(), binders.getKeyColumns()))
      map.put(p.getFieldName(), p);

    assertTrue(!binders.isEmpty());
    assertEquals(binders.getKeyColumns().size() + binders.getNonKeyColumns().size(), 10);

    assertTrue(map.containsKey("firstName"));
    assertTrue(map.get("firstName").getClass() == StringPreparedStatementBinder.class);

    assertTrue(map.containsKey("lastName"));
    assertTrue(map.get("lastName").getClass() == StringPreparedStatementBinder.class);

    assertTrue(map.containsKey("age"));
    assertTrue(map.get("age").getClass() == IntPreparedStatementBinder.class);

    assertTrue(map.get("long").getClass() == LongPreparedStatementBinder.class);
    assertEquals(((LongPreparedStatementBinder) map.get("long")).getValue(), l);

    assertTrue(map.get("short").getClass() == ShortPreparedStatementBinder.class);
    assertEquals(((ShortPreparedStatementBinder) map.get("short")).getValue(), s);

    assertTrue(map.get("byte").getClass() == BytePreparedStatementBinder.class);
    assertEquals(((BytePreparedStatementBinder) map.get("byte")).getValue(), b);

    assertTrue(map.get("float").getClass() == FloatPreparedStatementBinder.class);
    assertEquals(Float.compare(((FloatPreparedStatementBinder) map.get("float")).getValue(), f), 0);

    assertTrue(map.get("double").getClass() == DoublePreparedStatementBinder.class);
    assertEquals(Double.compare(((DoublePreparedStatementBinder) map.get("double")).getValue(), d), 0);

    assertTrue(map.get("bytes").getClass() == BytesPreparedStatementBinder.class);
    assertTrue(Arrays.equals(bs, ((BytesPreparedStatementBinder) map.get("bytes")).getValue()));
  }

  @Test
  public void returnAllFieldsAndApplyMappings() {


    Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

    double threshold = 215.66612;
    Struct struct = new Struct(schema)
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("age", 30)
            .put("threshold", threshold);

    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("lastName", new FieldAlias("Name"));
    mappings.put("age", new FieldAlias("a"));

    FieldsMappings tm = new FieldsMappings("table", "topic", true, mappings);
    StructFieldsDataExtractor dataExtractor = new StructFieldsDataExtractor(tm);
    StructFieldsDataExtractor.PreparedStatementBinders binders = dataExtractor.get(struct,
            new SinkRecord("", 1, null, null, schema, struct, 0));

    HashMap<String, PreparedStatementBinder> map = new HashMap<>();
    for (PreparedStatementBinder p : Iterables.concat(binders.getKeyColumns(), binders.getNonKeyColumns()))
      map.put(p.getFieldName(), p);


    assertTrue(!binders.isEmpty());
    assertEquals(binders.getKeyColumns().size() + binders.getNonKeyColumns().size(), 4);

    assertTrue(map.containsKey("firstName"));
    assertTrue(map.get("firstName").getClass() == StringPreparedStatementBinder.class);
    assertEquals(((StringPreparedStatementBinder) map.get("firstName")).getValue(), "Alex");

    assertTrue(map.containsKey("Name"));
    assertTrue(map.get("Name").getClass() == StringPreparedStatementBinder.class);
    assertEquals(((StringPreparedStatementBinder) map.get("Name")).getValue(), "Smith");

    assertTrue(map.containsKey("a"));
    assertTrue(map.get("a").getClass() == IntPreparedStatementBinder.class);
    assertEquals(((IntPreparedStatementBinder) map.get("a")).getValue(), 30);

    assertTrue(map.containsKey("threshold"));
    assertTrue(map.get("threshold").getClass() == DoublePreparedStatementBinder.class);
    assertTrue(Double.compare(((DoublePreparedStatementBinder) map.get("threshold")).getValue(), threshold) == 0);
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
            .put("age", 28);

    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("lastName", new FieldAlias("Name"));
    mappings.put("age", new FieldAlias("age"));

    FieldsMappings tm = new FieldsMappings("table", "topic", false, mappings);
    StructFieldsDataExtractor dataExtractor = new StructFieldsDataExtractor(tm);
    StructFieldsDataExtractor.PreparedStatementBinders binders = dataExtractor.get(struct,
            new SinkRecord("", 2, null, null, schema, struct, 2));

    HashMap<String, PreparedStatementBinder> map = new HashMap<>();
    for (PreparedStatementBinder p : Iterables.concat(binders.getNonKeyColumns(), binders.getKeyColumns()))
      map.put(p.getFieldName(), p);

    assertTrue(map.containsKey("Name"));
    assertTrue(map.get("Name").getClass() == StringPreparedStatementBinder.class);
    assertEquals(((StringPreparedStatementBinder) map.get("Name")).getValue(), "Smith");

    assertTrue(map.containsKey("age"));
    assertTrue(map.get("age").getClass() == IntPreparedStatementBinder.class);
    assertEquals(((IntPreparedStatementBinder) map.get("age")).getValue(), 28);
  }

  @Test
  public void shouldReturnThePrimaryKeysAtTheEndWhenMultipleFieldsFormThePrimaryKey() {
    Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("bool", Schema.BOOLEAN_SCHEMA)
            .field("short", Schema.INT16_SCHEMA)
            .field("byte", Schema.INT8_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

    short s = 1234;
    byte b = -32;
    long l = 12425436;
    float f = (float) 2356.3;
    double d = -2436546.56457;
    byte[] bs = new byte[]{-32, 124};

    Struct struct = new Struct(schema)
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("bool", true)
            .put("short", s)
            .put("byte", b)
            .put("long", l)
            .put("float", f)
            .put("double", d)
            .put("bytes", bs)
            .put("age", 30);

    Map<String, FieldAlias> mappings = new HashMap<>();
    mappings.put("firstName", new FieldAlias("fName", true));
    mappings.put("lastName", new FieldAlias("lName", true));

    FieldsMappings tm = new FieldsMappings("table", "topic", true, mappings);
    StructFieldsDataExtractor dataExtractor = new StructFieldsDataExtractor(tm);
    StructFieldsDataExtractor.PreparedStatementBinders binders = dataExtractor.get(struct,
            new SinkRecord("", 1, null, null, schema, struct, 0));

    HashMap<String, PreparedStatementBinder> map = new HashMap<>();
    for (PreparedStatementBinder p : Iterables.concat(binders.getNonKeyColumns(), binders.getKeyColumns()))
      map.put(p.getFieldName(), p);

    assertTrue(!binders.isEmpty());
    assertEquals(binders.getNonKeyColumns().size() + binders.getKeyColumns().size(), 10);

    List<PreparedStatementBinder> pkBinders = binders.getKeyColumns();
    assertEquals(pkBinders.size(), 2);

    assertTrue(Objects.equals(pkBinders.get(0).getFieldName(), "fName") ||
            Objects.equals(pkBinders.get(1).getFieldName(), "fName")
    );

    assertTrue(Objects.equals(pkBinders.get(0).getFieldName(), "lName") ||
            Objects.equals(pkBinders.get(1).getFieldName(), "lName")
    );

    assertTrue(map.containsKey("fName"));
    assertTrue(map.get("fName").getClass() == StringPreparedStatementBinder.class);

    assertTrue(map.containsKey("lName"));
    assertTrue(map.get("lName").getClass() == StringPreparedStatementBinder.class);

    assertTrue(map.containsKey("age"));
    assertTrue(map.get("age").getClass() == IntPreparedStatementBinder.class);

    assertTrue(map.get("long").getClass() == LongPreparedStatementBinder.class);
    assertEquals(((LongPreparedStatementBinder) map.get("long")).getValue(), l);

    assertTrue(map.get("short").getClass() == ShortPreparedStatementBinder.class);
    assertEquals(((ShortPreparedStatementBinder) map.get("short")).getValue(), s);

    assertTrue(map.get("byte").getClass() == BytePreparedStatementBinder.class);
    assertEquals(((BytePreparedStatementBinder) map.get("byte")).getValue(), b);

    assertTrue(map.get("float").getClass() == FloatPreparedStatementBinder.class);
    assertEquals(Float.compare(((FloatPreparedStatementBinder) map.get("float")).getValue(), f), 0);

    assertTrue(map.get("double").getClass() == DoublePreparedStatementBinder.class);
    assertEquals(Double.compare(((DoublePreparedStatementBinder) map.get("double")).getValue(), d), 0);

    assertTrue(map.get("bytes").getClass() == BytesPreparedStatementBinder.class);
    assertTrue(Arrays.equals(bs, ((BytesPreparedStatementBinder) map.get("bytes")).getValue()));
  }

  @Test
  public void shouldReturnThePrimaryKeysAtTheEndWhenOneFieldIsPK() {
    Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("bool", Schema.BOOLEAN_SCHEMA)
            .field("short", Schema.INT16_SCHEMA)
            .field("byte", Schema.INT8_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

    short s = 1234;
    byte b = -32;
    long l = 12425436;
    float f = (float) 2356.3;
    double d = -2436546.56457;
    byte[] bs = new byte[]{-32, 124};

    Struct struct = new Struct(schema)
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("bool", true)
            .put("short", s)
            .put("byte", b)
            .put("long", l)
            .put("float", f)
            .put("double", d)
            .put("bytes", bs)
            .put("age", 30);

    Map<String, FieldAlias> mappings = new HashMap<>();
    mappings.put("long", new FieldAlias("long", true));

    FieldsMappings tm = new FieldsMappings("table", "topic", true, mappings);

    StructFieldsDataExtractor dataExtractor = new StructFieldsDataExtractor(tm);
    StructFieldsDataExtractor.PreparedStatementBinders binders = dataExtractor.get(struct,
            new SinkRecord("", 2, null, null, schema, struct, 2));

    HashMap<String, PreparedStatementBinder> map = new HashMap<>();
    for (PreparedStatementBinder p : Iterables.concat(binders.getNonKeyColumns(), binders.getKeyColumns()))
      map.put(p.getFieldName(), p);

    assertTrue(!binders.isEmpty());
    assertEquals(map.size(), 10);

    assertTrue(Objects.equals(binders.getKeyColumns().get(0).getFieldName(), "long"));

    assertTrue(map.containsKey("firstName"));
    assertTrue(map.get("firstName").getClass() == StringPreparedStatementBinder.class);

    assertTrue(map.containsKey("lastName"));
    assertTrue(map.get("lastName").getClass() == StringPreparedStatementBinder.class);

    assertTrue(map.containsKey("age"));
    assertTrue(map.get("age").getClass() == IntPreparedStatementBinder.class);

    assertTrue(map.get("long").getClass() == LongPreparedStatementBinder.class);
    assertEquals(((LongPreparedStatementBinder) map.get("long")).getValue(), l);

    assertTrue(map.get("short").getClass() == ShortPreparedStatementBinder.class);
    assertEquals(((ShortPreparedStatementBinder) map.get("short")).getValue(), s);

    assertTrue(map.get("byte").getClass() == BytePreparedStatementBinder.class);
    assertEquals(((BytePreparedStatementBinder) map.get("byte")).getValue(), b);

    assertTrue(map.get("float").getClass() == FloatPreparedStatementBinder.class);
    assertEquals(Float.compare(((FloatPreparedStatementBinder) map.get("float")).getValue(), f), 0);

    assertTrue(map.get("double").getClass() == DoublePreparedStatementBinder.class);
    assertEquals(Double.compare(((DoublePreparedStatementBinder) map.get("double")).getValue(), d), 0);

    assertTrue(map.get("bytes").getClass() == BytesPreparedStatementBinder.class);
    assertTrue(Arrays.equals(bs, ((BytesPreparedStatementBinder) map.get("bytes")).getValue()));
  }
}
