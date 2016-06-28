package io.confluent.connect.jdbc.sink.writer;


import com.google.common.collect.Lists;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.SinkRecordField;
import io.confluent.connect.jdbc.sink.binders.BooleanPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.BytePreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.DoublePreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.FloatPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.IntPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.LongPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.PreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.ShortPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.StringPreparedStatementBinder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TablesToColumnUsageStateTest {
  @Test(expected = IllegalArgumentException.class)
  public void shouldRaiseAnExceptionIfTheTableNameIsNull() {
    new TablesToColumnUsageState().trackUsage(null, Lists.<PreparedStatementBinder>newArrayList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldRaiseAnExceptionIfTheTableNameIsWhitespace() {
    new TablesToColumnUsageState().trackUsage(" ", Lists.<PreparedStatementBinder>newArrayList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldRaiseAnExceptionIfTheBindersIsNull() {
    new TablesToColumnUsageState().trackUsage("table", null);
  }

  @Test
  public void shouldHandlesEmptyBinders() {
    TablesToColumnUsageState state = new TablesToColumnUsageState();
    state.trackUsage("table", Lists.<PreparedStatementBinder>newArrayList());

    assertEquals(state.getState().size(), 0);
  }

  @Test
  public void shouldHandleNewTable() {
    TablesToColumnUsageState state = new TablesToColumnUsageState();

    List<PreparedStatementBinder> cols = Lists.newArrayList();
    cols.add(new ShortPreparedStatementBinder("npk1", (short) 0));
    cols.add(new StringPreparedStatementBinder("npk2", "bibble"));
    cols.add(new BooleanPreparedStatementBinder("npk3", true));
    cols.add(new BytePreparedStatementBinder("npk4", (byte) 1));
    cols.add(new FloatPreparedStatementBinder("npk5", (float) 12.5));
    cols.add(new DoublePreparedStatementBinder("npk6", -1212.5));
    cols.add(new LongPreparedStatementBinder("npk7", 8823111));
    IntPreparedStatementBinder pk1 = new IntPreparedStatementBinder("pk1", 0);
    pk1.setPrimaryKey(true);
    cols.add(pk1);

    IntPreparedStatementBinder pk2 = new IntPreparedStatementBinder("pk2", 0);
    pk2.setPrimaryKey(true);
    cols.add(pk2);

    state.trackUsage("table", cols);

    Map<String, Collection<SinkRecordField>> s = state.getState();
    assertEquals(s.size(), 1);
    assertTrue(s.containsKey("table"));

    Map<String, SinkRecordField> fieldMap = new HashMap<>();
    for (SinkRecordField f : s.get("table")) {
      fieldMap.put(f.getName(), f);
    }

    assertEquals(cols.size(), fieldMap.size());
    assertTrue(fieldMap.containsKey("pk1"));
    assertTrue(fieldMap.containsKey("pk2"));
    assertTrue(fieldMap.containsKey("npk1"));
    assertTrue(fieldMap.containsKey("npk2"));
    assertTrue(fieldMap.containsKey("npk3"));
    assertTrue(fieldMap.containsKey("npk4"));
    assertTrue(fieldMap.containsKey("npk5"));
    assertTrue(fieldMap.containsKey("npk6"));
    assertTrue(fieldMap.containsKey("npk7"));

    assertEquals(Schema.Type.INT32, fieldMap.get("pk1").getType());
    assertEquals(true, fieldMap.get("pk1").isPrimaryKey());
    assertEquals(Schema.Type.INT32, fieldMap.get("pk2").getType());
    assertEquals(true, fieldMap.get("pk2").isPrimaryKey());

    assertEquals(Schema.Type.INT16, fieldMap.get("npk1").getType());
    assertEquals(Schema.Type.STRING, fieldMap.get("npk2").getType());
    assertEquals(Schema.Type.BOOLEAN, fieldMap.get("npk3").getType());
    assertEquals(Schema.Type.INT8, fieldMap.get("npk4").getType());
    assertEquals(Schema.Type.FLOAT32, fieldMap.get("npk5").getType());
    assertEquals(Schema.Type.FLOAT64, fieldMap.get("npk6").getType());
    assertEquals(Schema.Type.INT64, fieldMap.get("npk7").getType());

    for (int i = 1; i <= 7; ++i) {
      assertEquals(false, fieldMap.get("npk" + i).isPrimaryKey());
    }
  }

  @Test
  public void shouldHandleSecondRoundOfTrackingForTheSameFields() {

    TablesToColumnUsageState state = new TablesToColumnUsageState();

    List<PreparedStatementBinder> cols = Lists.newArrayList();
    cols.add(new ShortPreparedStatementBinder("npk1", (short) 0));
    cols.add(new StringPreparedStatementBinder("npk2", "bibble"));
    cols.add(new BooleanPreparedStatementBinder("npk3", true));
    cols.add(new BytePreparedStatementBinder("npk4", (byte) 1));
    cols.add(new FloatPreparedStatementBinder("npk5", (float) 12.5));
    cols.add(new DoublePreparedStatementBinder("npk6", -1212.5));
    cols.add(new LongPreparedStatementBinder("npk7", 8823111));
    cols.add(new IntPreparedStatementBinder("pk1", 0));
    cols.add(new IntPreparedStatementBinder("pk2", 0));

    state.trackUsage("table", cols);

    state.trackUsage("table", cols);

    Map<String, Collection<SinkRecordField>> s = state.getState();
    assertEquals(s.size(), 1);
    assertTrue(s.containsKey("table"));

    Map<String, SinkRecordField> fieldMap = new HashMap<>();
    for (SinkRecordField f : s.get("table")) {
      fieldMap.put(f.getName(), f);
    }

    assertEquals(cols.size(), fieldMap.size());
    assertTrue(fieldMap.containsKey("pk1"));
    assertTrue(fieldMap.containsKey("pk2"));
    assertTrue(fieldMap.containsKey("npk1"));
    assertTrue(fieldMap.containsKey("npk2"));
    assertTrue(fieldMap.containsKey("npk3"));
    assertTrue(fieldMap.containsKey("npk4"));
    assertTrue(fieldMap.containsKey("npk5"));
    assertTrue(fieldMap.containsKey("npk6"));
    assertTrue(fieldMap.containsKey("npk7"));
  }
}
