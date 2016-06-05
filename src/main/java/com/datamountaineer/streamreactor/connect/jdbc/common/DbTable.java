/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
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