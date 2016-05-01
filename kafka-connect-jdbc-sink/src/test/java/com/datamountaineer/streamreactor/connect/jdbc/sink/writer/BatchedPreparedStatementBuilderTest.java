package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;


import com.datamountaineer.streamreactor.connect.Pair;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.*;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.BatchedPreparedStatementBuilder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.PreparedStatementBuilder;
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
    public void groupAllRecordsWithTheSameColumns() throws SQLException {
        StructFieldsDataExtractor valueExtractor = mock(StructFieldsDataExtractor.class);
        List<Pair<String, PreparedStatementBinder>> dataBinders1 = Lists.newArrayList(
                new Pair<String, PreparedStatementBinder>("colA", new BooleanPreparedStatementBinder(true)),
                new Pair<String, PreparedStatementBinder>("colB", new IntPreparedStatementBinder(3)),
                new Pair<String, PreparedStatementBinder>("colC", new LongPreparedStatementBinder(124566)),
                new Pair<String, PreparedStatementBinder>("colD", new StringPreparedStatementBinder("somevalue")));

        List<Pair<String, PreparedStatementBinder>> dataBinders2 = Lists.newArrayList(
                new Pair<String, PreparedStatementBinder>("colE", new DoublePreparedStatementBinder(-5345.22)),
                new Pair<String, PreparedStatementBinder>("colF", new FloatPreparedStatementBinder(0)),
                new Pair<String, PreparedStatementBinder>("colG", new BytePreparedStatementBinder((byte) -24)),
                new Pair<String, PreparedStatementBinder>("colH", new ShortPreparedStatementBinder((short) -2345)));

        List<Pair<String, PreparedStatementBinder>> dataBinders3 = Lists.newArrayList(
                new Pair<String, PreparedStatementBinder>("A", new IntPreparedStatementBinder(1)),
                new Pair<String, PreparedStatementBinder>("B", new StringPreparedStatementBinder("bishbash")));

        when(valueExtractor.get(any(Struct.class))).
                thenReturn(dataBinders1, dataBinders2, dataBinders1, dataBinders1, dataBinders3);

        PreparedStatementBuilder builder = new BatchedPreparedStatementBuilder("tableA", valueExtractor);

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
}
