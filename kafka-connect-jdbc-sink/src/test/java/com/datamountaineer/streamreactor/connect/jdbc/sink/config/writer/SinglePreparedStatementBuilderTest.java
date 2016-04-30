package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer;


import com.datamountaineer.streamreactor.connect.Pair;
import com.datamountaineer.streamreactor.connect.jdbc.sink.IStructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.*;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.SinglePreparedStatementBuilder;
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class SinglePreparedStatementBuilderTest {
    @Test
    public void returnAPreparedStatementForEachRecord() throws SQLException {
        StructFieldsDataExtractor valueExtractor = mock(StructFieldsDataExtractor.class);
        List<Pair<String, PreparedStatementBinder>> dataBinders = Lists.newArrayList(
                new Pair<String, PreparedStatementBinder>("colA", new BooleanPreparedStatementBinder(true)),
                new Pair<String, PreparedStatementBinder>("colB", new IntPreparedStatementBinder(3)),
                new Pair<String, PreparedStatementBinder>("colC", new LongPreparedStatementBinder(124566)),
                new Pair<String, PreparedStatementBinder>("colD", new StringPreparedStatementBinder("somevalue")),
                new Pair<String, PreparedStatementBinder>("colE", new DoublePreparedStatementBinder(-5345.22)),
                new Pair<String, PreparedStatementBinder>("colF", new FloatPreparedStatementBinder(0)),
                new Pair<String, PreparedStatementBinder>("colG", new BytePreparedStatementBinder((byte) -24)),
                new Pair<String, PreparedStatementBinder>("colH", new ShortPreparedStatementBinder((short) -2345)));


        when(valueExtractor.get(any(Struct.class))).thenReturn(dataBinders);
        SinglePreparedStatementBuilder builder = new SinglePreparedStatementBuilder("tableA", valueExtractor);

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
        for(int i=0; i <10;++i)
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
}

