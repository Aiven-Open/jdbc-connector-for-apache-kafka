package io.confluent.connect.jdbc.sink.binders;

import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Handles binding Shorts for a prepared statement
 * */
public final class ShortPreparedStatementBinder extends BasePreparedStatementBinder {
  private final short value;

  public ShortPreparedStatementBinder(String name, short value) {
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
    statement.setShort(index, value);
  }

  /**
   * @return The value to be bound.
   * */
  public short getValue() {
    return value;
  }

  /**
   * Returns the field's schema type
   * @return Short
   */
  @Override
  public Schema.Type getFieldType() {
    return Schema.Type.INT16;
  }
}
