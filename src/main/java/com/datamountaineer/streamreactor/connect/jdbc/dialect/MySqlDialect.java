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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides support for MySql.
 */
public class MySqlDialect extends DbDialect {

  public MySqlDialect() {
    super(getSqlTypeMap(), "`", "`");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "TINYINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INT");
    map.put(Schema.Type.INT64, "BIGINT");
    map.put(Schema.Type.FLOAT32, "FLOAT");
    map.put(Schema.Type.FLOAT64, "DOUBLE");
    map.put(Schema.Type.BOOLEAN, "TINYINT");
    map.put(Schema.Type.STRING, "VARCHAR(256)");
    map.put(Schema.Type.BYTES, "VARBINARY(1024)");
    return map;
  }

  @Override
  public String getUpsertQuery(final String table, final List<String> cols, final List<String> keyCols) {
    if (table == null || table.trim().length() == 0)
      throw new IllegalArgumentException("<table=> is not valid. A non null non empty string expected");

    if (keyCols == null || keyCols.size() == 0) {
      throw new IllegalArgumentException(
              String.format("Your SQL table %s does not have any primary key/s. You can only UPSERT when your SQL table has primary key/s defined",
                      table));
    }
    List<String> nonKeyColumns = new ArrayList<>(cols.size());
    for (String c : cols) {
      nonKeyColumns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
    }

    List<String> keyColumns = new ArrayList<>(keyCols.size());
    for (String c : keyCols) {
      keyColumns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
    }
    //MySql doesn't support SQL 2003:merge so here how the upsert is handled
    final String queryColumns = Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns));
    final String bindingValues = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));

    final StringBuilder builder = new StringBuilder();
    builder.append("insert into ");
    builder.append(handleTableName(table));
    builder.append("(");
    builder.append(queryColumns);
    builder.append(") values(");
    builder.append(bindingValues);
    builder.append(") on duplicate key update ");

    builder.append(nonKeyColumns.get(0));
    builder.append("=values(");
    builder.append(nonKeyColumns.get(0));
    builder.append(")");
    for (int i = 1; i < nonKeyColumns.size(); ++i) {
      builder.append(",");
      builder.append(nonKeyColumns.get(i));
      builder.append("=values(");
      builder.append(nonKeyColumns.get(i));
      builder.append(")");
    }
    return builder.toString();
  }
}
