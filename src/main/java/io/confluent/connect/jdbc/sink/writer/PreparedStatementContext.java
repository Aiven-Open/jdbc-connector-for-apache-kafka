package io.confluent.connect.jdbc.sink.writer;

import java.util.Collection;
import java.util.Map;

import io.confluent.connect.jdbc.sink.SinkRecordField;

/**
 * Contains a list of PreparedStatements to execute as well as the tables affected and the columns referenced.
 */
public class PreparedStatementContext {
  private final PreparedStatementData preparedStatementData;
  private final Map<String, Collection<SinkRecordField>> tablesToColumnsMap;

  public PreparedStatementContext(PreparedStatementData preparedStatementData,
                                  Map<String, Collection<SinkRecordField>> tablesToColumnsMap) {
    this.preparedStatementData = preparedStatementData;
    this.tablesToColumnsMap = tablesToColumnsMap;
  }

  /**
   * Returns the list of PreparedStatements to execute
   *
   * @return Returns the list of PreparedStatements to execute
   */
  public PreparedStatementData getPreparedStatementData() {
    return preparedStatementData;
  }

  /**
   * Returns a map of table name to fields/columns involved
   *
   * @return Returns a map of table name to fields/columns involved
   */
  public Map<String, Collection<SinkRecordField>> getTablesToColumnsMap() {
    return tablesToColumnsMap;
  }
}
