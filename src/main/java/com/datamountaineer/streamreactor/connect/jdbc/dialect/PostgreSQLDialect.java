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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by andrew@datamountaineer.com on 17/05/16.
 * kafka-connect-jdbc
 * <p>
 * The user is responsible for escaping the columns otherwise create table A and create table "A" is not the same
 */
public class PostgreSQLDialect extends DbDialect {
  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLDialect.class);

  public PostgreSQLDialect() {
    super(getSqlTypeMap(), "\"", "\"");
  }


  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "SMALLINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INT");
    map.put(Schema.Type.INT64, "BIGINT");
    map.put(Schema.Type.FLOAT32, "FLOAT");
    map.put(Schema.Type.FLOAT64, "DOUBLE PRECISION");
    map.put(Schema.Type.BOOLEAN, "BOOLEAN");
    map.put(Schema.Type.STRING, "TEXT");
    map.put(Schema.Type.BYTES, "BYTEA");
    return map;
  }

  @Override
  protected String handleTableName(String tableName) {
    return tableName;
  }

  @Override
  public String getUpsertQuery(final String table, final List<String> cols, final List<String> keyCols) {
    if (table == null || table.trim().length() == 0)
      throw new IllegalArgumentException("<table=> is not valid. A non null non empty string expected");

    if (keyCols == null || keyCols.size() == 0) {
      throw new IllegalArgumentException(
              String.format("Your SQL table %s does not have any primary key/s. You can only UPSERT when your SQL table has primary key/s defined",
                      table)
      );
    }

    List<String> nonKeyColumns = new ArrayList<>(cols.size());
    for (String c : cols) {
      nonKeyColumns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
    }

    List<String> keyColumns = new ArrayList<>(keyCols.size());
    for (String c : keyCols) {
      keyColumns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
    }

    final String queryColumns = Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns));
    final String bindingValues = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));

    String updateSet = null;
    if (nonKeyColumns.size() > 0) {
      final StringBuilder updateSetBuilder = new StringBuilder();
      updateSetBuilder.append(String.format("%s=EXCLUDED.%s", nonKeyColumns.get(0), nonKeyColumns.get(0)));
      for (int i = 1; i < nonKeyColumns.size(); ++i) {
        updateSetBuilder.append(String.format(",%s=EXCLUDED.%s", nonKeyColumns.get(i), nonKeyColumns.get(i)));
      }
      updateSet = updateSetBuilder.toString();
    }

    return "INSERT INTO " + handleTableName(table) + " (" + queryColumns + ") " +
            "VALUES (" + bindingValues + ") " +
            "ON CONFLICT (" + Joiner.on(",").join(keyColumns) + ") DO UPDATE SET " + updateSet;

  }
}
