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
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
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


    when(valueExtractor.get(any(Struct.class)))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders,
                    Lists.<PreparedStatementBinder>newArrayList()));
    SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder("tableA",
            valueExtractor,
            new InsertQueryBuilder());

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct record = new Struct(schema);
    List<SinkRecord> records = Collections.nCopies(10, new SinkRecord("aa", 1, null, null, schema, record, 0));

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


    when(valueExtractor.get(any(Struct.class)))
            .thenReturn(new StructFieldsDataExtractor.PreparedStatementBinders(dataBinders,
                    Lists.<PreparedStatementBinder>newArrayList(new IntPreparedStatementBinder("colPK", 1010101))));
    SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder("tableA",
            valueExtractor,
            new UpsertQueryBuilder(new MySqlDialect()));

    //schema is not used as we mocked the value extractors
    Schema schema = SchemaBuilder.struct().name("record")
            .version(1)
            .field("id", Schema.STRING_SCHEMA)
            .build();

    Struct record = new Struct(schema);
    List<SinkRecord> records = Collections.nCopies(10, new SinkRecord("aa", 1, null, null, schema, record, 0));

    Connection connection = mock(Connection.class);
    String sql = "insert into tableA(colA,colB,colC,colD,colE,colF,colG,colH,colPK) values(?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update colA=values(colA),colB=values(colB),colC=values(colC),"+
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
}

