/**
 * Copyright 2015 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

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

      when(valueExtractor.get(any(Struct.class))).
              thenReturn(
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1, Lists.<PreparedStatementBinder>newArrayList()),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders2, Lists.<PreparedStatementBinder>newArrayList()),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1, Lists.<PreparedStatementBinder>newArrayList()),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1, Lists.<PreparedStatementBinder>newArrayList()),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders3, Lists.<PreparedStatementBinder>newArrayList()));

      PreparedStatementBuilder builder = new BatchedPreparedStatementBuilder("tableA",
              valueExtractor,
              new InsertQueryBuilder());

      //schema is not used as we mocked the value extractors
      Schema schema = SchemaBuilder.struct().name("record")
              .version(1)
              .field("id", Schema.STRING_SCHEMA)
              .build();


      Struct record = new Struct(schema);

      //same size as the valueextractor.get returns
      Collection<SinkRecord> records = Collections.nCopies(5, new SinkRecord("aa", 1, null, null, schema, record, 0));

      Connection connection = mock(Connection.class);

      String sql1 = "INSERT INTO tableA(colA,colB,colC,colD) VALUES(?,?,?,?)";
      PreparedStatement statement1 = mock(PreparedStatement.class);
      when(connection.prepareStatement(sql1)).thenReturn(statement1);

      String sql2 = "INSERT INTO tableA(colE,colF,colG,colH) VALUES(?,?,?,?)";
      PreparedStatement statement2 = mock(PreparedStatement.class);
      when(connection.prepareStatement(sql2)).thenReturn(statement2);


      String sql3 = "INSERT INTO tableA(A,B) VALUES(?,?)";
      PreparedStatement statement3 = mock(PreparedStatement.class);
      when(connection.prepareStatement(sql3)).thenReturn(statement3);

      List<PreparedStatement> actualStatements = builder.build(records, connection);

      assertEquals(actualStatements.size(), 3);

      verify(connection, times(1)).prepareStatement(sql1);
      verify(connection, times(1)).prepareStatement(sql2);
      verify(connection, times(1)).prepareStatement(sql3);

      verify(statement1, times(3)).setBoolean(1, true);
      verify(statement1, times(3)).setInt(2, 3);
      verify(statement1, times(3)).setLong(3, 124566);
      verify(statement1, times(3)).setString(4, "somevalue");
      verify(statement1, times(3)).addBatch();

      verify(statement2, times(1)).setDouble(1, -5345.22);
      verify(statement2, times(1)).setFloat(2, 0);
      verify(statement2, times(1)).setByte(3, (byte) -24);
      verify(statement2, times(1)).setShort(4, (short) -2345);
      verify(statement2, times(1)).addBatch();

      verify(statement3, times(1)).setInt(1, 1);
      verify(statement3, times(1)).setString(2, "bishbash");
      verify(statement3, times(1)).addBatch();
  }

  @Test
  public void groupAllRecordsWithTheSameColumnsForMySqlUpsert() throws SQLException {
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


      PreparedStatementBinder pk1 = new IntPreparedStatementBinder("colPK", 1);
      PreparedStatementBinder pk2 = new IntPreparedStatementBinder("colPK", 2);
      PreparedStatementBinder pk3 = new IntPreparedStatementBinder("colPK", 3);

      List<PreparedStatementBinder> dataBinders3 = Lists.<PreparedStatementBinder>newArrayList(
              new IntPreparedStatementBinder("A", 1),
              new StringPreparedStatementBinder("B", "bishbash"));

      when(valueExtractor.get(any(Struct.class))).
              thenReturn(
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1, Lists.newArrayList(pk1)),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders2, Lists.newArrayList(pk2)),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1, Lists.newArrayList(pk1)),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders1, Lists.newArrayList(pk1)),
                      new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders3, Lists.newArrayList(pk3)));

      PreparedStatementBuilder builder = new BatchedPreparedStatementBuilder("tableA",
              valueExtractor,
              new UpsertQueryBuilder(new MySqlDialect()));

      //schema is not used as we mocked the value extractors
      Schema schema = SchemaBuilder.struct().name("record")
              .version(1)
              .field("id", Schema.STRING_SCHEMA)
              .build();


      Struct record = new Struct(schema);

      //same size as the valueextractor.get returns
      Collection<SinkRecord> records = Collections.nCopies(5, new SinkRecord("aa", 1, null, null, schema, record, 0));

      Connection connection = mock(Connection.class);

      String sql1 = "insert into tableA(colA,colB,colC,colD,colPK) values(?,?,?,?,?) " +
              "on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC),colD=values(colD)";
      PreparedStatement statement1 = mock(PreparedStatement.class);
      when(connection.prepareStatement(sql1)).thenReturn(statement1);

      String sql2 = "insert into tableA(colE,colF,colG,colH,colPK) values(?,?,?,?,?) " +
              "on duplicate key update colE=values(colE),colF=values(colF),colG=values(colG),colH=values(colH)";
      PreparedStatement statement2 = mock(PreparedStatement.class);
      when(connection.prepareStatement(sql2)).thenReturn(statement2);


      String sql3 = "insert into tableA(A,B,colPK) values(?,?,?) " +
              "on duplicate key update A=values(A),B=values(B)";
      PreparedStatement statement3 = mock(PreparedStatement.class);
      when(connection.prepareStatement(sql3)).thenReturn(statement3);

      List<PreparedStatement> actualStatements = builder.build(records, connection);

      assertEquals(actualStatements.size(), 3);

      verify(connection, times(1)).prepareStatement(sql1);
      verify(connection, times(1)).prepareStatement(sql2);
      verify(connection, times(1)).prepareStatement(sql3);

      verify(statement1, times(3)).setBoolean(1, true);
      verify(statement1, times(3)).setInt(2, 3);
      verify(statement1, times(3)).setLong(3, 124566);
      verify(statement1, times(3)).setString(4, "somevalue");
      verify(statement1, times(3)).addBatch();

      verify(statement2, times(1)).setDouble(1, -5345.22);
      verify(statement2, times(1)).setFloat(2, 0);
      verify(statement2, times(1)).setByte(3, (byte) -24);
      verify(statement2, times(1)).setShort(4, (short) -2345);
      verify(statement2, times(1)).addBatch();

      verify(statement3, times(1)).setInt(1, 1);
      verify(statement3, times(1)).setString(2, "bishbash");
      verify(statement3, times(1)).addBatch();
  }
}
