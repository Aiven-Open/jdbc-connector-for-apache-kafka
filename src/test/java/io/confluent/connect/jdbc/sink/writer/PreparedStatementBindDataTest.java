package io.confluent.connect.jdbc.sink.writer;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import io.confluent.connect.jdbc.sink.binders.BooleanPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.BytePreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.DoublePreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.FloatPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.IntPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.LongPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.PreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.ShortPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.StringPreparedStatementBinder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PreparedStatementBindDataTest {
  @Test
  public void bindAllGivenValuesToTheSqlStatement() throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    List<PreparedStatementBinder> values = Arrays.<PreparedStatementBinder>asList(
        new BooleanPreparedStatementBinder("", true),
        new BytePreparedStatementBinder("", (byte) 8),
        new ShortPreparedStatementBinder("", (byte) -24),
        new IntPreparedStatementBinder("", 3),
        new LongPreparedStatementBinder("", 612111),
        new FloatPreparedStatementBinder("", (float) 15.12),
        new DoublePreparedStatementBinder("", -235426.6677),
        new StringPreparedStatementBinder("", "some value")
    );

    PreparedStatementBindData.apply(statement, values);

    verify(statement, times(1)).setBoolean(1, true);
    verify(statement, times(1)).setByte(2, (byte) 8);
    verify(statement, times(1)).setShort(3, (byte) -24);
    verify(statement, times(1)).setInt(4, 3);
    verify(statement, times(1)).setLong(5, 612111);
    verify(statement, times(1)).setFloat(6, (float) 15.12);
    verify(statement, times(1)).setDouble(7, -235426.6677);
    verify(statement, times(1)).setString(8, "some value");
  }
}
