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
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/***
 * Contains the database table information.
 */
public class DbTable {
  private final String name;
  private final List<DbTableColumn> columns;

  public DbTable(String name, List<DbTableColumn> columns) {
    ParameterValidator.notNullOrEmpty(name, "name");
    ParameterValidator.notNull(columns, "columns");
    this.name = name;
    this.columns = columns;
  }

  public String getName() {
    return name;
  }

  public void addColumn(final DbTableColumn column) {
    ParameterValidator.notNull(column, "column");
    final Optional<DbTableColumn> found = Iterables.tryFind(columns, new Predicate<DbTableColumn>() {
      @Override
      public boolean apply(DbTableColumn col) {
        return Objects.equals(col.getName(), column.getName());
      }
    });
    if (found.isPresent()) {
      throw new IllegalArgumentException(String.format("%s column is already present", column.getName()));
    }
    columns.add(column);
  }


  public Map<String, DbTableColumn> getColumns() {
    final Map<String, DbTableColumn> map = new HashMap<>();
    for (final DbTableColumn column : columns) {
      map.put(column.getName(), column);
    }
    return map;
  }
}