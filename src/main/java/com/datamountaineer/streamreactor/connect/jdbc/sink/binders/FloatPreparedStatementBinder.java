package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Handles binding Floats for a prepared statement
 * */
public final class FloatPreparedStatementBinder extends BasePreparedStatementBinder {
  private final float value;

  public FloatPreparedStatementBinder(String name, float value) {
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
    statement.setFloat(index, value);
  }

  /**
   * @return The value to be bound.
   * */
  public float getValue() {
    return value;
  }

  /**
   * Returns the field's schema type
   * @return Float
   */
  @Override
  public Schema.Type getFieldType() {
    return Schema.Type.FLOAT32;
  }
}
