package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;


import com.datamountaineer.streamreactor.connect.jdbc.sink.Field;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BooleanPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BytePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.DoublePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.FloatPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.IntPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.LongPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.ShortPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.StringPreparedStatementBinder;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TablesToColumnUsageStateTest {
  @Test(expected = IllegalArgumentException.class)
  public void shouldRaiseAnExceptionIfTheTableNameIsNull() {
    new TablesToColumnUsageState().trackUsage(null,
            new StructFieldsDataExtractor.PreparedStatementBinders(Lists.<PreparedStatementBinder>newArrayList(),
                    Lists.<PreparedStatementBinder>newArrayList()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldRaiseAnExceptionIfTheTableNameIsWhitespace() {
    new TablesToColumnUsageState().trackUsage(" ",
            new StructFieldsDataExtractor.PreparedStatementBinders(Lists.<PreparedStatementBinder>newArrayList(),
                    Lists.<PreparedStatementBinder>newArrayList()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldRaiseAnExceptionIfTheBindersIsNull() {
    new TablesToColumnUsageState().trackUsage("table", null);
  }

  @Test
  public void shouldHandlesNullPkColumns() {
    new TablesToColumnUsageState().trackUsage("table",
            new StructFieldsDataExtractor.PreparedStatementBinders(Lists.<PreparedStatementBinder>newArrayList(), null));
  }

  @Test
  public void shouldHandlesNullNonPKColumns() {
    new TablesToColumnUsageState().trackUsage("table",
            new StructFieldsDataExtractor.PreparedStatementBinders(null, Lists.<PreparedStatementBinder>newArrayList()));
  }

  @Test
  public void shouldHandlesEmptyBinders() {
    TablesToColumnUsageState state = new TablesToColumnUsageState();
    state.trackUsage("table",
            new StructFieldsDataExtractor.PreparedStatementBinders(Lists.<PreparedStatementBinder>newArrayList(),
                    Lists.<PreparedStatementBinder>newArrayList()));

    assertEquals(state.getState().size(), 0);
  }

  @Test
  public void shouldHandleNewTable() {
    TablesToColumnUsageState state = new TablesToColumnUsageState();

    List<PreparedStatementBinder> pkCols = new ArrayList<>();
    pkCols.add(new IntPreparedStatementBinder("pk1", 0));
    pkCols.add(new IntPreparedStatementBinder("pk2", 0));

    List<PreparedStatementBinder> nonPKCols = Lists.newArrayList();
    nonPKCols.add(new ShortPreparedStatementBinder("npk1", (short) 0));
    nonPKCols.add(new StringPreparedStatementBinder("npk2", "bibble"));
    nonPKCols.add(new BooleanPreparedStatementBinder("npk3", true));
    nonPKCols.add(new BytePreparedStatementBinder("npk4", (byte) 1));
    nonPKCols.add(new FloatPreparedStatementBinder("npk5", (float) 12.5));
    nonPKCols.add(new DoublePreparedStatementBinder("npk6", -1212.5));
    nonPKCols.add(new LongPreparedStatementBinder("npk7", 8823111));

    state.trackUsage("table",
            new StructFieldsDataExtractor.PreparedStatementBinders(nonPKCols, pkCols));

    Map<String, Collection<Field>> s = state.getState();
    assertEquals(s.size(), 1);
    assertTrue(s.containsKey("table"));

    Map<String, Field> fieldMap = new HashMap<>();
    for (Field f : s.get("table")) {
      fieldMap.put(f.getName(), f);
    }

    assertEquals(pkCols.size() + nonPKCols.size(), fieldMap.size());
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

    List<PreparedStatementBinder> pkCols = new ArrayList<>();
    pkCols.add(new IntPreparedStatementBinder("pk1", 0));
    pkCols.add(new IntPreparedStatementBinder("pk2", 0));

    List<PreparedStatementBinder> nonPKCols = Lists.newArrayList();
    nonPKCols.add(new ShortPreparedStatementBinder("npk1", (short) 0));
    nonPKCols.add(new StringPreparedStatementBinder("npk2", "bibble"));
    nonPKCols.add(new BooleanPreparedStatementBinder("npk3", true));
    nonPKCols.add(new BytePreparedStatementBinder("npk4", (byte) 1));
    nonPKCols.add(new FloatPreparedStatementBinder("npk5", (float) 12.5));
    nonPKCols.add(new DoublePreparedStatementBinder("npk6", -1212.5));
    nonPKCols.add(new LongPreparedStatementBinder("npk7", 8823111));

    state.trackUsage("table",
            new StructFieldsDataExtractor.PreparedStatementBinders(nonPKCols, pkCols));

    state.trackUsage("table",
            new StructFieldsDataExtractor.PreparedStatementBinders(nonPKCols, pkCols));

    Map<String, Collection<Field>> s = state.getState();
    assertEquals(s.size(), 1);
    assertTrue(s.containsKey("table"));

    Map<String, Field> fieldMap = new HashMap<>();
    for (Field f : s.get("table")) {
      fieldMap.put(f.getName(), f);
    }

    assertEquals(pkCols.size() + nonPKCols.size(), fieldMap.size());
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
