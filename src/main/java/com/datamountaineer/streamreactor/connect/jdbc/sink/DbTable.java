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
package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * Contains the database table information.
 */
public class DbTable {
  private final String name;
  private final Map<String, DbTableColumn> columnMap = new HashMap<>();

  public DbTable(String name, List<DbTableColumn> columns) {
    ParameterValidator.notNullOrEmpty(name, "name");
    ParameterValidator.notNull(columns, "columns");
    this.name = name;
    for (final DbTableColumn column : columns) {
      columnMap.put(column.getName(), column);
    }
  }

  public String getName() {
    return name;
  }

  public void addColumn(final DbTableColumn column) {
    ParameterValidator.notNull(column, "column");

    if (columnMap.containsKey(column.getName())) {
      throw new IllegalArgumentException(String.format("%s column is already present", column.getName()));
    }
    getColumns().put(column.getName(), column);
  }


  public Map<String, DbTableColumn> getColumns() {
    return columnMap;
  }

  public boolean containsColumn(final String tableName) {
    return columnMap.containsKey(tableName);
  }
}