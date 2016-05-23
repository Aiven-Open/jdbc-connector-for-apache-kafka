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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchedPreparedStatementBuilderTest {

  @Test
  public void groupAllRecordsWithTheSameColumnsForInsertQuery() throws SQLException {
    StructFieldsDataExtractor valueExtractor = mock(StructFieldsDataExtractor.class);
    List<PreparedStatementBinder> dataBinders1 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"));

    List<PreparedStatementBinder> dataBinders2 = Lists.<PreparedStatementBinder>newArrayList(
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345));

    List<PreparedStatementBinder> dataBinders3 = Lists.<PreparedStatementBinder>newArrayList(
            new IntPreparedStatementBinder("A", 1),
            new StringPreparedStatementBinder("B", "bishbash"));

    when(valueExtractor.getTableName()).thenReturn("tableA");
    when(valueExtractor.get(any(Struct.class), any(SinkRecord.class))).
            thenReturn(dataBinders1,
                    dataBinders2,
                    dataBinders1,
                    dataBinders1,
                    dataBinders3);

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put("topic1a", valueExtractor);
    PreparedStatementBuilder builder = new BatchedPreparedStatementBuilder(map, new InsertQueryBuilder());

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();


    Struct record = new Struct(schema);

    //same size as the valueextractor.get returns
    Collection<SinkRecord> records = Collections.nCopies(5, new SinkRecord("topic1a", 1, null, null, schema, record, 0));

    String sql1 = "INSERT INTO tableA(colA,colB,colC,colD) VALUES(?,?,?,?)";
    String sql2 = "INSERT INTO tableA(colE,colF,colG,colH) VALUES(?,?,?,?)";

    String sql3 = "INSERT INTO tableA(A,B) VALUES(?,?)";

    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());

    Map<String, PreparedStatementData> dataMap = new HashMap<>();
    for (PreparedStatementData d : actualStatements) {
      dataMap.put(d.getSql(), d);
    }
    assertEquals(actualStatements.size(), 3);

    assertTrue(dataMap.containsKey(sql1));
    assertTrue(dataMap.containsKey(sql2));
    assertTrue(dataMap.containsKey(sql3));

    List<Iterable<PreparedStatementBinder>> binders = dataMap.get(sql1).getBinders();
    assertEquals(3, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
    }

    binders = dataMap.get(sql2).getBinders();
    assertEquals(1, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(0, Double.compare(-5345.22, ((DoublePreparedStatementBinder) b.get(0)).getValue()));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(1)).getValue(), 0));
      assertEquals(-24, ((BytePreparedStatementBinder) b.get(2)).getValue());
      assertEquals(-2345, ((ShortPreparedStatementBinder) b.get(3)).getValue());
    }

    binders = dataMap.get(sql3).getBinders();
    assertEquals(1, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(1, ((IntPreparedStatementBinder) b.get(0)).getValue());
      assertEquals("bishbash", ((StringPreparedStatementBinder) b.get(1)).getValue());
    }
  }

  @Test
  public void handleMultipleTablesForInsert() throws SQLException {
    StructFieldsDataExtractor valueExtractor1 = mock(StructFieldsDataExtractor.class);
    List<PreparedStatementBinder> dataBinders1 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"));

    StructFieldsDataExtractor valueExtractor2 = mock(StructFieldsDataExtractor.class);
    List<PreparedStatementBinder> dataBinders2 = Lists.<PreparedStatementBinder>newArrayList(
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345));


    when(valueExtractor1.getTableName()).thenReturn("tableA");
    when(valueExtractor2.getTableName()).thenReturn("tableB");

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct struct1 = new Struct(schema);
    when(valueExtractor1.get(eq(struct1), any(SinkRecord.class))).
            thenReturn(dataBinders1);

    Struct struct2 = new Struct(schema);
    when(valueExtractor2.get(eq(struct2), any(SinkRecord.class))).
            thenReturn(dataBinders2);


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put("topic1a", valueExtractor1);
    map.put("topic2a", valueExtractor2);

    PreparedStatementBuilder builder = new BatchedPreparedStatementBuilder(map, new InsertQueryBuilder());

    Collection<SinkRecord> records = Lists.newArrayList(
            new SinkRecord("topic1a", 1, null, null, schema, struct1, 0),
            new SinkRecord("topic2a", 1, null, null, schema, struct2, 0),
            new SinkRecord("topic1a", 1, null, null, schema, struct1, 0),
            new SinkRecord("topic1a", 1, null, null, schema, struct1, 0));

    String sql1 = "INSERT INTO tableA(colA,colB,colC,colD) VALUES(?,?,?,?)";

    String sql2 = "INSERT INTO tableB(colE,colF,colG,colH) VALUES(?,?,?,?)";

    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());
    assertEquals(actualStatements.size(), 2);

    assertEquals(sql1, actualStatements.get(0).getSql());
    assertEquals(sql2, actualStatements.get(1).getSql());

    assertEquals(3, actualStatements.get(0).getBinders().size());

    List<Iterable<PreparedStatementBinder>> binders = actualStatements.get(0).getBinders();
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
    }

    binders = actualStatements.get(1).getBinders();
    assertEquals(1, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(0));
      assertEquals(0, Double.compare(-5345.22, ((DoublePreparedStatementBinder) b.get(0)).getValue()));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(1)).getValue(), 0));
      assertEquals((byte) -24, ((BytePreparedStatementBinder) b.get(2)).getValue());
      assertEquals(-2345, ((ShortPreparedStatementBinder) b.get(3)).getValue());
    }
  }

  @Test
  public void groupAllRecordsWithTheSameColumnsForMySqlUpsert() throws SQLException {
    StructFieldsDataExtractor valueExtractor = mock(StructFieldsDataExtractor.class);

    IntPreparedStatementBinder pk1 = new IntPreparedStatementBinder("colPK", 1);
    pk1.setPrimaryKey(true);

    IntPreparedStatementBinder pk2 = new IntPreparedStatementBinder("colPK", 2);
    pk2.setPrimaryKey(true);

    IntPreparedStatementBinder pk3 = new IntPreparedStatementBinder("colPK", 3);
    pk3.setPrimaryKey(true);

    List<PreparedStatementBinder> dataBinders1 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            pk1);

    List<PreparedStatementBinder> dataBinders2 = Lists.<PreparedStatementBinder>newArrayList(
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345),
            pk2);


    List<PreparedStatementBinder> dataBinders3 = Lists.<PreparedStatementBinder>newArrayList(
            new IntPreparedStatementBinder("A", 1),
            new StringPreparedStatementBinder("B", "bishbash"),
            pk3);

    String table = "tableA";
    String topic = "topic1Ab";
    when(valueExtractor.getTableName()).thenReturn(table);
    when(valueExtractor.get(any(Struct.class), any(SinkRecord.class))).
            thenReturn(dataBinders1,
                    dataBinders2,
                    dataBinders1,
                    dataBinders1,
                    dataBinders3);

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic.toLowerCase(), valueExtractor);

    QueryBuilder queryBuilder = new UpsertQueryBuilder(new MySqlDialect());
    PreparedStatementBuilder builder = new BatchedPreparedStatementBuilder(map, queryBuilder);

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();


    Struct record = new Struct(schema);

    //same size as the valueextractor.get returns
    Collection<SinkRecord> records = Collections.nCopies(5, new SinkRecord(topic, 1, null, null, schema, record, 0));

    Connection connection = mock(Connection.class);

    String sql1 = "insert into tableA(colA,colB,colC,colD,colPK) values(?,?,?,?,?) " +
            "on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC),colD=values(colD)";
    PreparedStatement statement1 = mock(PreparedStatement.class);
    when(connection.prepareStatement(sql1)).thenReturn(statement1);

    String sql2 = "insert into tableA(colE,colF,colG,colH,colPK) values(?,?,?,?,?) " +
            "on duplicate key update colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";

    String sql3 = "insert into tableA(A,B,colPK) values(?,?,?) " +
            "on duplicate key update A=values(A),B=values(B)";

    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());

    Map<String, PreparedStatementData> dataMap = new HashMap<>();
    for (PreparedStatementData d : actualStatements) {
      dataMap.put(d.getSql(), d);
    }
    assertEquals(actualStatements.size(), 3);

    assertTrue(dataMap.containsKey(sql1));
    assertTrue(dataMap.containsKey(sql2));
    assertTrue(dataMap.containsKey(sql3));

    List<Iterable<PreparedStatementBinder>> binders = dataMap.get(sql1).getBinders();
    assertEquals(3, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
    }

    binders = dataMap.get(sql2).getBinders();
    assertEquals(1, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(0, Double.compare(-5345.22, ((DoublePreparedStatementBinder) b.get(0)).getValue()));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(1)).getValue(), 0));
      assertEquals((byte) -24, ((BytePreparedStatementBinder) b.get(2)).getValue());
      assertEquals(-2345, ((ShortPreparedStatementBinder) b.get(3)).getValue());
    }

    binders = dataMap.get(sql3).getBinders();
    assertEquals(1, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(1, ((IntPreparedStatementBinder) b.get(0)).getValue());
      assertEquals("bishbash", ((StringPreparedStatementBinder) b.get(1)).getValue());
    }
  }

  @Test
  public void handleMultipleTablesForUpsert() throws SQLException {
    StructFieldsDataExtractor valueExtractor1 = mock(StructFieldsDataExtractor.class);

    IntPreparedStatementBinder pk1 = new IntPreparedStatementBinder("colPK", 0);
    pk1.setPrimaryKey(true);

    IntPreparedStatementBinder pk2 = new IntPreparedStatementBinder("colPK", 0);
    pk2.setPrimaryKey(true);

    List<PreparedStatementBinder> dataBinders1 = Lists.<PreparedStatementBinder>newArrayList(
            new BooleanPreparedStatementBinder("colA", true),
            new IntPreparedStatementBinder("colB", 3),
            new LongPreparedStatementBinder("colC", 124566),
            new StringPreparedStatementBinder("colD", "somevalue"),
            pk1);

    StructFieldsDataExtractor valueExtractor2 = mock(StructFieldsDataExtractor.class);
    List<PreparedStatementBinder> dataBinders2 = Lists.<PreparedStatementBinder>newArrayList(
            new DoublePreparedStatementBinder("colE", -5345.22),
            new FloatPreparedStatementBinder("colF", 0),
            new BytePreparedStatementBinder("colG", (byte) -24),
            new ShortPreparedStatementBinder("colH", (short) -2345),
            pk2);


    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct struct1 = new Struct(schema);
    String table1 = "tableA";
    String topic1 = "topic1Ab";

    when(valueExtractor1.getTableName()).thenReturn(table1);
    when(valueExtractor1.get(eq(struct1), any(SinkRecord.class))).
            thenReturn(dataBinders1);

    Struct struct2 = new Struct(schema);
    String table2 = "tableB";
    String topic2 = "topic2";

    when(valueExtractor2.getTableName()).thenReturn(table2);
    when(valueExtractor2.get(eq(struct2), any(SinkRecord.class))).
            thenReturn(dataBinders2);


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic1.toLowerCase(), valueExtractor1);
    map.put(topic2.toLowerCase(), valueExtractor2);

    QueryBuilder queryBuilder = new UpsertQueryBuilder(new MySqlDialect());
    PreparedStatementBuilder builder = new BatchedPreparedStatementBuilder(map, queryBuilder);

    //same size as the valueextractor.get returns
    Collection<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic1, 1, null, null, schema, struct1, 0),
            new SinkRecord(topic2, 1, null, null, schema, struct2, 0),
            new SinkRecord(topic1, 1, null, null, schema, struct1, 0),
            new SinkRecord(topic1, 1, null, null, schema, struct1, 0));

    String sql1 = "insert into tableA(colA,colB,colC,colD,colPK) values(?,?,?,?,?) " +
            "on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC),colD=values(colD)";

    String sql2 = "insert into tableB(colE,colF,colG,colH,colPK) values(?,?,?,?,?) " +
            "on duplicate key update colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";

    List<PreparedStatementData> actualStatements = Lists.newArrayList(builder.build(records).getPreparedStatements());

    Map<String, PreparedStatementData> dataMap = new HashMap<>();
    for (PreparedStatementData d : actualStatements) {
      dataMap.put(d.getSql(), d);
    }
    assertEquals(actualStatements.size(), 2);

    assertTrue(dataMap.containsKey(sql1));
    assertTrue(dataMap.containsKey(sql2));

    List<Iterable<PreparedStatementBinder>> binders = dataMap.get(sql2).getBinders();
    assertEquals(1, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(0, Double.compare(-5345.22, ((DoublePreparedStatementBinder) b.get(0)).getValue()));
      assertEquals(0, Float.compare(((FloatPreparedStatementBinder) b.get(1)).getValue(), 0));
      assertEquals(-24, ((BytePreparedStatementBinder) b.get(2)).getValue());
      assertEquals(-2345, ((ShortPreparedStatementBinder) b.get(3)).getValue());
    }

    binders = dataMap.get(sql1).getBinders();
    assertEquals(3, binders.size());
    for (int i = 0; i < binders.size(); ++i) {
      List<PreparedStatementBinder> b = Lists.newArrayList(binders.get(i));
      assertEquals(true, ((BooleanPreparedStatementBinder) b.get(0)).getValue());
      assertEquals(3, ((IntPreparedStatementBinder) b.get(1)).getValue());
      assertEquals(124566, ((LongPreparedStatementBinder) b.get(2)).getValue());
      assertEquals("somevalue", ((StringPreparedStatementBinder) b.get(3)).getValue());
    }
  }
}
