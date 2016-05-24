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

package com.datamountaineer.streamreactor.connect.jdbc.dialect;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Sql2003Dialect extends DbDialect {

  Sql2003Dialect(final Map<Schema.Type, String> map, String escapeColumnNamesStart, String escapeColumnNamesEnd) {
    super(map, escapeColumnNamesStart, escapeColumnNamesEnd);
  }

  @Override
  public String getUpsertQuery(String table, List<String> cols, List<String> keyCols) {
    if (table == null || table.trim().length() == 0)
      throw new IllegalArgumentException("<table> is not valid");

    if (keyCols == null || keyCols.size() == 0) {
      throw new IllegalArgumentException("<keyColumns> is not valid. It has to be non null and non empty.");
    }

    List<String> columns = null;
    if (cols != null) {
      columns = new ArrayList<>(cols.size());
      for (String c : cols) {
        columns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
      }
    }
    List<String> keyColumns = new ArrayList<>(keyCols.size());
    for (String c : keyCols) {
      keyColumns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
    }
    final String select = Joiner.on(", ? ").join(Iterables.concat(columns, keyColumns));
    final StringBuilder joinBuilder = new StringBuilder();
    joinBuilder.append(String.format("%s.%s=incoming.%s", table, keyColumns.get(0), keyColumns.get(0)));
    for (int i = 1; i < keyColumns.size(); ++i) {
      joinBuilder.append(String.format(" and %s.%s=incoming.%s", table, keyColumns.get(i), keyColumns.get(i)));
    }

    String updateSet = null;
    if (columns != null && columns.size() > 0) {
      final StringBuilder updateSetBuilder = new StringBuilder("when matched then update set ");
      updateSetBuilder.append(String.format("%s.%s=incoming.%s", table, columns.get(0), columns.get(0)));
      for (int i = 1; i < columns.size(); ++i) {
        updateSetBuilder.append(String.format(",%s.%s=incoming.%s", table, columns.get(i), columns.get(i)));
      }
      updateSet = updateSetBuilder.toString();
    }

    final String insertColumns = Joiner.on(String.format(",%s.", table)).join(Iterables.concat(columns, keyColumns));
    final String insertValues = Joiner.on(",incoming.").join(Iterables.concat(columns, keyColumns));

        /*
        https://blogs.oracle.com/cmar/entry/using_merge_to_do_an"
         */
    return "merge into " + table + getMergeHints() +
            " using (select ? " + select + ") incoming" +
            " on(" + joinBuilder.toString() + ") " +
            updateSet +
            String.format(" when not matched then insert(%s.%s) values(incoming.%s)",
                    table,
                    insertColumns,
                    insertValues);

  }

  String getMergeHints() {
    return "";
  }
}