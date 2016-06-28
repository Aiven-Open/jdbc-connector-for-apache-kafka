package io.confluent.connect.jdbc.sink.writer;

import io.confluent.connect.jdbc.sink.binders.PreparedStatementBinder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Binds the SinkRecord entries to the sql PreparedStatement.
 */
final class PreparedStatementBindData {
  /**
   * Binds the values to the given PreparedStatement
   *
   * @param statement - the sql prepared statement to be executed
   * @param binders   -  The SinkRecord values to be bound to the sql statement
   */
  public static void apply(PreparedStatement statement, Iterable<PreparedStatementBinder> binders) throws SQLException {

    int index = 1;
    for (final PreparedStatementBinder binder : binders) {
      binder.bind(index++, statement);
    }
  }
}
