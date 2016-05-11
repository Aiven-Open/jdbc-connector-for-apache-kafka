package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.*;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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
    when(valueExtractor.get(any(Struct.class)))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders,
                    Lists.<PreparedStatementBinder>newArrayList()));

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

    Connection connection = mock(Connection.class);
    String sql = "INSERT INTO tableA(colA,colB,colC,colD,colE,colF,colG,colH) VALUES(?,?,?,?,?,?,?,?)";

    List<PreparedStatement> statements = Lists.newArrayList();
    for (int i = 0; i < 10; ++i)
      statements.add(mock(PreparedStatement.class));

    PreparedStatement[] returnedStatementJavaSyntaxIsAncient = new PreparedStatement[statements.size() - 1];
    for (int i = 1; i < statements.size(); ++i)
      returnedStatementJavaSyntaxIsAncient[i - 1] = statements.get(i);
    when(connection.prepareStatement(sql))
            .thenReturn(statements.get(0), returnedStatementJavaSyntaxIsAncient);

    List<PreparedStatement> actualStatements = builder.build(records, connection);

    assertEquals(actualStatements.size(), 10);
    verify(connection, times(10)).prepareStatement(sql);

    for (final PreparedStatement s : statements) {

      verify(s, times(1)).setBoolean(1, true);
      verify(s, times(1)).setInt(2, 3);
      verify(s, times(1)).setLong(3, 124566);
      verify(s, times(1)).setString(4, "somevalue");
      verify(s, times(1)).setDouble(5, -5345.22);
      verify(s, times(1)).setFloat(6, 0);
      verify(s, times(1)).setByte(7, (byte) -24);
      verify(s, times(1)).setShort(8, (short) -2345);
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

    when(valueExtractor1.get(record1))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1,
                    Lists.<PreparedStatementBinder>newArrayList()));

    Struct record2 = new Struct(schema);
    SinkRecord sinkRecord2 = new SinkRecord(topic2, 1, null, null, schema, record2, 0);

    when(valueExtractor2.get(record2))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders2,
                    Lists.<PreparedStatementBinder>newArrayList()));


    List<SinkRecord> records = Lists.newArrayList(sinkRecord1, sinkRecord2);

    Connection connection = mock(Connection.class);
    String sql1 = "INSERT INTO tableA(colA,colB,colC,colD,colE,colF,colG,colH) VALUES(?,?,?,?,?,?,?,?)";

    String sql2 = "INSERT INTO tableB(colA,colB,colC,colD,colE,colF,colG,colH) VALUES(?,?,?,?,?,?,?,?)";


    PreparedStatement preparedStatement1 = mock(PreparedStatement.class);
    when(connection.prepareStatement(sql1)).thenReturn(preparedStatement1);

    PreparedStatement preparedStatement2 = mock(PreparedStatement.class);
    when(connection.prepareStatement(sql2)).thenReturn(preparedStatement2);

    List<PreparedStatement> actualStatements = builder.build(records, connection);

    assertEquals(actualStatements.size(), 2);
    verify(connection, times(1)).prepareStatement(sql1);
    verify(connection, times(1)).prepareStatement(sql2);

    for (final PreparedStatement s : new PreparedStatement[]{preparedStatement1, preparedStatement2}) {
      verify(s, times(1)).setBoolean(1, true);
      verify(s, times(1)).setInt(2, 3);
      verify(s, times(1)).setLong(3, 124566);
      verify(s, times(1)).setString(4, "somevalue");
      verify(s, times(1)).setDouble(5, -5345.22);
      verify(s, times(1)).setFloat(6, 0);
      verify(s, times(1)).setByte(7, (byte) -24);
      verify(s, times(1)).setShort(8, (short) -2345);
    }
  }


  @Test
  public void returnAPreparedStatementForEachRecordForUpsertQuery() throws SQLException {
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
    when(valueExtractor.get(any(Struct.class)))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders,
                    Lists.<PreparedStatementBinder>newArrayList(new IntPreparedStatementBinder("colPK", 1010101))));

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

    Connection connection = mock(Connection.class);
    String sql = "insert into tableA(colA,colB,colC,colD,colE,colF,colG,colH,colPK) values(?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC)," +
            "colD=values(colD),colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";

    List<PreparedStatement> statements = Lists.newArrayList();
    for (int i = 0; i < 10; ++i)
      statements.add(mock(PreparedStatement.class));

    PreparedStatement[] returnedStatementJavaSyntaxIsAncient = new PreparedStatement[statements.size() - 1];
    for (int i = 1; i < statements.size(); ++i)
      returnedStatementJavaSyntaxIsAncient[i - 1] = statements.get(i);
    when(connection.prepareStatement(sql))
            .thenReturn(statements.get(0), returnedStatementJavaSyntaxIsAncient);

    List<PreparedStatement> actualStatements = builder.build(records, connection);

    assertEquals(actualStatements.size(), 10);
    verify(connection, times(10)).prepareStatement(sql);

    for (final PreparedStatement s : statements) {

      verify(s, times(1)).setBoolean(1, true);
      verify(s, times(1)).setInt(2, 3);
      verify(s, times(1)).setLong(3, 124566);
      verify(s, times(1)).setString(4, "somevalue");
      verify(s, times(1)).setDouble(5, -5345.22);
      verify(s, times(1)).setFloat(6, 0);
      verify(s, times(1)).setByte(7, (byte) -24);
      verify(s, times(1)).setShort(8, (short) -2345);
      verify(s, times(1)).setInt(9, 1010101);
    }
  }

  @Test
  public void handleMultipleTablesForUpsert() throws SQLException {
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
    when(valueExtractor1.get(record1))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1,
                    Lists.<PreparedStatementBinder>newArrayList(new IntPreparedStatementBinder("colPK", 1010101))));

    Struct record2 = new Struct(schema);
    when(valueExtractor2.get(record2))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders2,
                    Lists.<PreparedStatementBinder>newArrayList(new IntPreparedStatementBinder("colPK", 2020202))));


    SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder(map, new UpsertQueryBuilder(new MySqlDialect()));


    List<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic1, 1, null, null, schema, record1, 0),
            new SinkRecord(topic2, 1, null, null, schema, record2, 0),
            new SinkRecord(topic1, 1, null, null, schema, record1, 0));

    Connection connection = mock(Connection.class);
    String sql1 = "insert into tableA(colA,colB,colC,colD,colE,colF,colG,colH,colPK) values(?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC)," +
            "colD=values(colD),colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";

    String sql2 = "insert into tableB(colA,colB,colC,colD,colE,colF,colG,colH,colPK) values(?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC)," +
            "colD=values(colD),colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";

    PreparedStatement preparedStatement1 = mock(PreparedStatement.class);
    PreparedStatement preparedStatement1a = mock(PreparedStatement.class);
    PreparedStatement preparedStatement2 = mock(PreparedStatement.class);

    when(connection.prepareStatement(sql1)).thenReturn(preparedStatement1, preparedStatement1a);
    when(connection.prepareStatement(sql2)).thenReturn(preparedStatement2);

    List<PreparedStatement> actualStatements = builder.build(records, connection);

    assertEquals(actualStatements.size(), 3);
    verify(connection, times(2)).prepareStatement(sql1);
    verify(connection, times(1)).prepareStatement(sql2);

    for (final PreparedStatement s : new PreparedStatement[]{preparedStatement1, preparedStatement1a, preparedStatement2}) {

      verify(s, times(1)).setBoolean(1, true);
      verify(s, times(1)).setInt(2, 3);
      verify(s, times(1)).setLong(3, 124566);
      verify(s, times(1)).setString(4, "somevalue");
      verify(s, times(1)).setDouble(5, -5345.22);
      verify(s, times(1)).setFloat(6, 0);
      verify(s, times(1)).setByte(7, (byte) -24);
      verify(s, times(1)).setShort(8, (short) -2345);

    }
    verify(preparedStatement1, times(1)).setInt(9, 1010101);
    verify(preparedStatement1a, times(1)).setInt(9, 1010101);
    verify(preparedStatement2, times(1)).setInt(9, 2020202);
  }
}

