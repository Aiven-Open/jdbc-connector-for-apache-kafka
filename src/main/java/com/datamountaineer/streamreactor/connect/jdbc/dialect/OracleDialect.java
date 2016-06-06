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

import com.datamountaineer.streamreactor.connect.jdbc.common.ParameterValidator;
import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides support for Oracle database
 */
public class OracleDialect extends DbDialect {
  public OracleDialect() {
    super(getSqlTypeMap(), "\"", "\"");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "TINYINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INTEGER");
    map.put(Schema.Type.INT64, "NUMBER(19)");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "BINARY_DOUBLE");
    map.put(Schema.Type.BOOLEAN, "NUMBER(1,0)");
    map.put(Schema.Type.STRING, "VARCHAR(256)");
    map.put(Schema.Type.BYTES, "BLOB");
    return map;
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    ParameterValidator.notNullOrEmpty(tableName, "table");
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is empty.");
    }
    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(handleTableName(tableName)); //yes oracles needs it uppercase
    builder.append(" ADD(");

    boolean first = true;
    for (final SinkRecordField f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(lineSeparator);
      builder.append(escapeColumnNamesStart)
              .append(f.getName())
              .append(escapeColumnNamesEnd);
      builder.append(" ");
      builder.append(getSqlType(f.getType()));
      builder.append(" NULL");
    }
    builder.append(")");

    final List<String> query = new ArrayList<String>(1);
    query.add(builder.toString());
    return query;
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

    final Iterable<String> iter = Iterables.concat(columns, keyColumns);
    final String select = Joiner.on(", ? ").join(iter);
    final StringBuilder builder = new StringBuilder();
    builder.append("merge into ");
    String tableName = handleTableName(table);
    builder.append(tableName);
    builder.append(" using (select ? ");
    builder.append(select);
    builder.append(" FROM dual) incoming on(");
    builder.append(tableName);
    builder.append(".");
    builder.append(keyColumns.get(0));
    builder.append("=incoming.");
    builder.append(keyColumns.get(0));
    for (int i = 1; i < keyColumns.size(); ++i) {
      builder.append(" and ");
      builder.append(tableName);
      builder.append(".");
      builder.append(keyColumns.get(i));
      builder.append("=incoming.");
      builder.append(keyColumns.get(i));
    }
    builder.append(")");
    if (columns != null && columns.size() > 0) {
      builder.append(" when matched then update set ");
      builder.append(tableName);
      builder.append(".");
      builder.append(columns.get(0));
      builder.append("=incoming.");
      builder.append(columns.get(0));
      for (int i = 1; i < columns.size(); ++i) {
        builder.append(",");
        builder.append(tableName);
        builder.append(".");
        builder.append(columns.get(i));
        builder.append("=incoming.");
        builder.append(columns.get(i));
      }
    }

    final String insertColumns = Joiner.on(String.format(",%s.", tableName)).join(iter);
    final String insertValues = Joiner.on(",incoming.").join(iter);

    builder.append(" when not matched then insert(");
    builder.append(tableName);
    builder.append(".");
    builder.append(insertColumns);
    builder.append(") values(incoming.");
    builder.append(insertValues);
    builder.append(")");
        /*
        https://blogs.oracle.com/cmar/entry/using_merge_to_do_an"
         */
    return builder.toString();

  }

  @Override
  protected String handleTableName(String tableName) {
    return escapeColumnNamesStart + tableName.toUpperCase() + escapeColumnNamesEnd;
  }
}
