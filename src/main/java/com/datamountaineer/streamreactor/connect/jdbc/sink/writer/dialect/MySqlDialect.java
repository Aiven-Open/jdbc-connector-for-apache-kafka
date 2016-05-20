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
import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides support for MySql.
 */
public class MySqlDialect extends DbDialect {

  public MySqlDialect() {
    super(getSqlTypeMap());
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
  public String getUpsertQuery(final String table, final List<String> nonKeyColumns, final List<String> keyColumns) {
    if (table == null || table.trim().length() == 0)
      throw new IllegalArgumentException("<table=> is not valid. A non null non empty string expected");

    if (keyColumns == null || keyColumns.size() == 0) {
      throw new IllegalArgumentException("<keyColumns> is invalid. Need to be non null, non empty and be a subset of <columns>");
    }
    //MySql doesn't support SQL 2003:merge so here how the upsert is handled
    final String queryColumns = Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns));
    final String bindingValues = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));

    final StringBuilder builder = new StringBuilder();
    builder.append(String.format("%s=values(%s)", nonKeyColumns.get(0), nonKeyColumns.get(0)));
    for (int i = 1; i < nonKeyColumns.size(); ++i) {
      builder.append(String.format(",%s=values(%s)", nonKeyColumns.get(i), nonKeyColumns.get(i)));
    }

    return String.format("insert into %s(%s) values(%s) " +
            "on duplicate key update %s", table, queryColumns, bindingValues, builder.toString());
  }
}
