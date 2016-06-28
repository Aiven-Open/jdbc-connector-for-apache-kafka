package io.confluent.connect.jdbc.sink.binders;

import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Interface for creating bindings to a prepared statement
 */
public interface PreparedStatementBinder {

  /**
   * Returns true if the field is a primary key one
   */
  boolean isPrimaryKey();

  /**
   * Returns the column name for which the value is inserted
   *
   * @return Column name for inserted
   */
  String getFieldName();

  /**
   * Returns the Kafka Connect field Schema type
   *
   * @return Topic schema
   */
  Schema.Type getFieldType();

  /**
   * Bind the value to the prepared statement.
   *
   * @param index The ordinal position to bind the variable to.
   * @param statement The prepared statement to bind to.
   */

  void bind(int index, PreparedStatement statement) throws SQLException;
}
