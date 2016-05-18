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

package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.List;

/**
 * Created by andrew@datamountaineer.com on 17/05/16.
 * kafka-connect-jdbc
 */
public class PostgreDialect extends DbDialect {
  @Override
  public String getUpsertQuery(String table, List<String> columns, List<String> keyColumns) {
    if (table == null || table.trim().length() == 0)
      throw new IllegalArgumentException("<table> is not valid");

    if (keyColumns == null || keyColumns.size() == 0) {
      throw new IllegalArgumentException("<keyColumns> is not valid. It has to be non null and non empty.");
    }

    String updateSet = null;
    if (columns.size() > 0) {
      final StringBuilder updateSetBuilder = new StringBuilder("");
      updateSetBuilder.append(String.format("%s.%s=incoming.%s", table, columns.get(0), columns.get(0)));
      for (int i = 1; i < columns.size(); ++i) {
        updateSetBuilder.append(String.format(",%s.%s=incoming.%s", table, columns.get(i), columns.get(i)));
      }
      updateSet = updateSetBuilder.toString();
    }

    final String insertColumns = Joiner.on(String.format(",%s.", table)).join(Iterables.concat(columns, keyColumns));
    final String insertValues = Joiner.on(",incoming.").join(Iterables.concat(columns, keyColumns));
    final String key = Joiner.on(",").join(keyColumns);

    String sql = "INSERT INTO " + table + "(" + insertColumns + ") " +
                 "VALUES (" + insertValues + ") " +
                 "ON CONFLICT (" + key + ") UPDATE " + updateSet;

    return sql;
  }

  public String getMergeHints() {
    return "";
  }



}
