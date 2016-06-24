package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Handles binding Doubles for a prepared statement
 * */
public final class DoublePreparedStatementBinder extends BasePreparedStatementBinder {
  private final double value;

  public DoublePreparedStatementBinder(String name, double value) {
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
    statement.setDouble(index, value);
  }

  /**
   * @return The value to be bound.
   * */
  public double getValue() {
    return value;
  }

  /**
   * Returns the field's schema type
   * @return Float64
   */
  @Override
  public Schema.Type getFieldType() {
    return Schema.Type.FLOAT64;
  }
}
