package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

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
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.MySqlDialect;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SinglePreparedStatementBuilderTest {
  @Test
  public void returnAPreparedStatementForEachRecordForInsertQuery() throws SQLException {
    StructFieldsDataExtractor valueExtractor = mock(StructFieldsDataExtractor.class);
    List<PreparedStatementBinder> dataBinders = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345));

    String topic = "tableA";

    when(valueExtractor.getTableName()).thenReturn(topic);
    when(valueExtractor.get(any(Struct.class), any(SinkRecord.class)))
            .thenReturn(dataBinders);

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic.toLowerCase(), valueExtractor);

    SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder(map, new InsertQueryBuilder());

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct record = new Struct(schema);
    List<SinkRecord> records = Collections.nCopies(10, new SinkRecord(topic, 1, null, null, schema, record, 0));

    String sql = "INSERT INTO tableA(colA,colB,colC,colD,colE,colF,colG,colH) VALUES(?,?,?,?,?,?,?,?)";

    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());

    assertEquals(actualStatements.size(), 10);


    for (final PreparedStatementData d : actualStatements) {
      List<Iterable<PreparedStatementBinder>> e = d.getBinders();
      assertEquals(sql, d.getSql());
      List<PreparedStatementBinder> b = Lists.newArrayList(e.get(0));

      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
      assertEquals(0, Double.compare(((DoublePreparedStatementBinder) b.get(4)).getValue(), -5345.22));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(5)).getValue(), 0));
      assertEquals((byte) -24, ((BytePreparedStatementBinder) b.get(6)).getValue());
      assertEquals((short) -2345, ((ShortPreparedStatementBinder) b.get(7)).getValue());
    }
  }

  @Test
  public void handleMultipleTablesForInsert() throws SQLException {
    StructFieldsDataExtractor valueExtractor1 = mock(StructFieldsDataExtractor.class);
    List<PreparedStatementBinder> dataBinders1 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345));

    StructFieldsDataExtractor valueExtractor2 = mock(StructFieldsDataExtractor.class);
    List<PreparedStatementBinder> dataBinders2 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345));

    String topic1 = "topic1A";
    String table1 = "tableA";
    String topic2 = "topic2";
    String table2 = "tableB";


    when(valueExtractor1.getTableName()).thenReturn(table1);
    when(valueExtractor2.getTableName()).thenReturn(table2);


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic1.toLowerCase(), valueExtractor1);
    map.put(topic2.toLowerCase(), valueExtractor2);

    SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder(map, new InsertQueryBuilder());

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct record1 = new Struct(schema);
    SinkRecord sinkRecord1 = new SinkRecord(topic1, 1, null, null, schema, record1, 0);

    when(valueExtractor1.get(eq(record1), any(SinkRecord.class)))
            .thenReturn(dataBinders1);

    Struct record2 = new Struct(schema);
    SinkRecord sinkRecord2 = new SinkRecord(topic2, 1, null, null, schema, record2, 0);

    when(valueExtractor2.get(eq(record2), any(SinkRecord.class)))
            .thenReturn(dataBinders2);


    List<SinkRecord> records = Lists.newArrayList(sinkRecord1, sinkRecord2);

    String sql1 = "INSERT INTO tableA(colA,colB,colC,colD,colE,colF,colG,colH) VALUES(?,?,?,?,?,?,?,?)";

    String sql2 = "INSERT INTO tableB(colA,colB,colC,colD,colE,colF,colG,colH) VALUES(?,?,?,?,?,?,?,?)";


    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());

    assertEquals(actualStatements.size(), 2);
    assertEquals(sql1, actualStatements.get(0).getSql());
    assertEquals(sql2, actualStatements.get(1).getSql());


    for (final PreparedStatementData d : actualStatements) {
      List<Iterable<PreparedStatementBinder>> e = d.getBinders();
      List<PreparedStatementBinder> b = Lists.newArrayList(e.get(0));

      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
      assertEquals(0, Double.compare(((DoublePreparedStatementBinder) b.get(4)).getValue(), -5345.22));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(5)).getValue(), 0));
      assertEquals((byte) -24, ((BytePreparedStatementBinder) b.get(6)).getValue());
      assertEquals((short) -2345, ((ShortPreparedStatementBinder) b.get(7)).getValue());
    }
  }


  @Test
  public void returnAPreparedStatementForEachRecordForUpsertQuery() throws SQLException {
    StructFieldsDataExtractor valueExtractor = mock(StructFieldsDataExtractor.class);
    IntPreparedStatementBinder pk = new IntPreparedStatementBinder("colPK", 1010101);
    pk.setPrimaryKey(true);
    List<PreparedStatementBinder> dataBinders = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345),
            pk);

    String topic = "tableA";

    when(valueExtractor.getTableName()).thenReturn(topic);
    when(valueExtractor.get(any(Struct.class), any(SinkRecord.class)))
            .thenReturn(dataBinders);

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic.toLowerCase(), valueExtractor);

    SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder(map, new UpsertQueryBuilder(new MySqlDialect()));

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct record = new Struct(schema);
    List<SinkRecord> records = Collections.nCopies(10, new SinkRecord(topic, 1, null, null, schema, record, 0));

    String sql = "insert into tableA(colA,colB,colC,colD,colE,colF,colG,colH,colPK) values(?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC)," +
            "colD=values(colD),colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";

    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());

    assertEquals(actualStatements.size(), 10);

    for (final PreparedStatementData d : actualStatements) {
      List<Iterable<PreparedStatementBinder>> e = d.getBinders();
      assertEquals(sql, d.getSql());
      List<PreparedStatementBinder> b = Lists.newArrayList(e.get(0));

      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
      assertEquals(0, Double.compare(((DoublePreparedStatementBinder) b.get(4)).getValue(), -5345.22));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(5)).getValue(), 0));
      assertEquals((byte) -24, ((BytePreparedStatementBinder) b.get(6)).getValue());
      assertEquals((short) -2345, ((ShortPreparedStatementBinder) b.get(7)).getValue());
      assertEquals(1010101, ((IntPreparedStatementBinder) b.get(8)).getValue());
    }
  }

  @Test
  public void handleMultipleTablesForUpsert() throws SQLException {
    StructFieldsDataExtractor valueExtractor1 = mock(StructFieldsDataExtractor.class);
    IntPreparedStatementBinder pk1 = new IntPreparedStatementBinder("colPK", 1010101);
    pk1.setPrimaryKey(true);
    List<PreparedStatementBinder> dataBinders1 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345),
            pk1);

    StructFieldsDataExtractor valueExtractor2 = mock(StructFieldsDataExtractor.class);
    IntPreparedStatementBinder pk2 = new IntPreparedStatementBinder("colPK", 2020202);
    pk2.setPrimaryKey(true);
    List<PreparedStatementBinder> dataBinders2 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345),
            pk2);

    String topic1 = "topic1A";
    String table1 = "tableA";
    when(valueExtractor1.getTableName()).thenReturn(table1);

    String topic2 = "topic2A";
    String table2 = "tableB";
    when(valueExtractor2.getTableName()).thenReturn(table2);

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic1.toLowerCase(), valueExtractor1);
    map.put(topic2.toLowerCase(), valueExtractor2);

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct record1 = new Struct(schema);
    when(valueExtractor1.get(eq(record1), any(SinkRecord.class)))
            .thenReturn(dataBinders1);

    Struct record2 = new Struct(schema);
    when(valueExtractor2.get(eq(record2), any(SinkRecord.class)))
            .thenReturn(dataBinders2);

    SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder(map, new UpsertQueryBuilder(new MySqlDialect()));


    List<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic1, 1, null, null, schema, record1, 0),
            new SinkRecord(topic2, 1, null, null, schema, record2, 0),
            new SinkRecord(topic1, 1, null, null, schema, record1, 0));

    String sql1 = "insert into tableA(colA,colB,colC,colD,colE,colF,colG,colH,colPK) values(?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC)," +
            "colD=values(colD),colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";

    String sql2 = "insert into tableB(colA,colB,colC,colD,colE,colF,colG,colH,colPK) values(?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC)," +
            "colD=values(colD),colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";


    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());

    assertEquals(actualStatements.size(), 3);
    assertEquals(sql1, actualStatements.get(0).getSql());
    assertEquals(sql2, actualStatements.get(1).getSql());
    assertEquals(sql1, actualStatements.get(2).getSql());

    int i = 0;
    for (final PreparedStatementData d : actualStatements) {
      List<Iterable<PreparedStatementBinder>> e = d.getBinders();
      List<PreparedStatementBinder> b = Lists.newArrayList(e.get(0));

      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
      assertEquals(0, Double.compare(((DoublePreparedStatementBinder) b.get(4)).getValue(), -5345.22));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(5)).getValue(), 0));
      assertEquals((byte) -24, ((BytePreparedStatementBinder) b.get(6)).getValue());
      assertEquals((short) -2345, ((ShortPreparedStatementBinder) b.get(7)).getValue());

      if (i++ != 1) {
        assertEquals(1010101, ((IntPreparedStatementBinder) b.get(8)).getValue());
      } else {
        assertEquals(2020202, ((IntPreparedStatementBinder) b.get(8)).getValue());
      }
    }
  }
}

