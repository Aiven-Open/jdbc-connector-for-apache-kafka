package io.confluent.connect.jdbc.sink.binders;

import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * Handles binding Longs for a prepared statement
 * */
public final class LongPreparedStatementBinder extends BasePreparedStatementBinder {
  private final long value;

  public LongPreparedStatementBinder(final String name, long value) {
    super(name);
    this.value = value;
  }

  /**
   * Bind the value to the prepared statement.
   *
   * @param index The ordinal position to bind the variable to.
   * @param statement The prepared statement to bind to.
   * */
  @Override
  public void bind(int index, PreparedStatement statement) throws SQLException {
    statement.setLong(index, value);
  }

  /**
   * @return The value to be bound.
   * */
  public long getValue() {
    return value;
  }

  /**
   * Returns the field's schema type
   * @return Long
   */
  @Override
  public Schema.Type getFieldType() {
    return Schema.Type.INT64;
  }
}
