package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Handles binding a Byte for a prepared statement
 * */
public final class BytePreparedStatementBinder extends BasePreparedStatementBinder {
  private final byte value;

  public BytePreparedStatementBinder(String name, byte value) {
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
    statement.setByte(index, value);
  }

  /**
   * @return The value to be bound.
   * */
  public byte getValue() {
    return value;
  }

  /**
   * Returns the field's schema type
   * @return Byte
   */
  @Override
  public Schema.Type getFieldType() {
    return Schema.Type.INT8;
  }
}
