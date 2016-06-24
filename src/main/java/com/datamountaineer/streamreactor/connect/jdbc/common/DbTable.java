package com.datamountaineer.streamreactor.connect.jdbc.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * Contains the database table information.
 */
public class DbTable {
  private final String name;
  private final Map<String, DbTableColumn> columnMap = new HashMap<>();

  /**
   * Creates a new instance of DbTable
   *
   * @param name    - The table name
   * @param columns - Contains all the table columns information
   */
  public DbTable(String name, List<DbTableColumn> columns) {
    ParameterValidator.notNullOrEmpty(name, "name");
    ParameterValidator.notNull(columns, "columns");
    this.name = name;
    for (final DbTableColumn column : columns) {
      columnMap.put(column.getName(), column);
    }
  }

  /**
   * Returns the table name
   *
   * @return The table name
   */
  public String getName() {
    return name;
  }

  /**
   * A map of columns present in the table
   *
   * @return Columns map
   */
  public Map<String, DbTableColumn> getColumns() {
    return columnMap;
  }

  /**
   * Checks if the column is present
   *
   * @param columnName - The column to check if it is present
   * @return true - the table already contains the column; false-otherwise
   */
  public boolean containsColumn(final String columnName) {
    return columnMap.containsKey(columnName);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ name: ");
    builder.append(name);
    builder.append(", columns:[");
    for (Map.Entry<String, DbTableColumn> e : columnMap.entrySet()) {
      builder.append(e.toString());
      builder.append(",");
    }
    builder.append("]}");
    return builder.toString();
  }
}