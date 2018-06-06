/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A description of a table.
 */
public class TableDefinition {
  private final TableId id;
  private final Map<String, ColumnDefinition> columnsByName = new LinkedHashMap<>();
  private final Map<String, String> pkColumnNames = new LinkedHashMap<>();

  public TableDefinition(
      TableId id,
      Iterable<ColumnDefinition> columns
  ) {
    this.id = id;
    for (ColumnDefinition defn : columns) {
      String columnName = defn.id().name();
      columnsByName.put(
          columnName,
          defn.forTable(this.id)
      );
      if (defn.isPrimaryKey()) {
        this.pkColumnNames.put(
            columnName,
            columnName
        );
      }
    }
  }

  public TableId id() {
    return id;
  }

  public int columnCount() {
    return columnsByName.size();
  }

  public ColumnDefinition definitionForColumn(String name) {
    return columnsByName.get(name);
  }

  public Collection<ColumnDefinition> definitionsForColumns() {
    return columnsByName.values();
  }

  public Collection<String> primaryKeyColumnNames() {
    return pkColumnNames.values();
  }

  public Set<String> columnNames() {
    return columnsByName.keySet();
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return true;
    }
    if (obj instanceof TableDefinition) {
      TableDefinition that = (TableDefinition) obj;
      return this.id.equals(that.id()) && this.definitionsForColumns()
                                              .equals(that.definitionsForColumns());
    }
    return false;
  }

  @Override
  public String toString() {
    return "Table{" + "name='" + id + '\'' + ", columns=" + definitionsForColumns() + '}';
  }
}
