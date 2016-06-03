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

public class SqlServerDialect extends Sql2003Dialect {

  public SqlServerDialect() {
    super(getSqlTypeMap(), "[", "]");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "tinyint");
    map.put(Schema.Type.INT16, "smallint");
    map.put(Schema.Type.INT32, "int");
    map.put(Schema.Type.INT64, "bigint");
    map.put(Schema.Type.FLOAT32, "real");
    map.put(Schema.Type.FLOAT64, "float");
    map.put(Schema.Type.BOOLEAN, "bit");
    map.put(Schema.Type.STRING, "varchar(256)");
    map.put(Schema.Type.BYTES, "varbinary(1024)");
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
    builder.append(handleTableName(tableName));
    builder.append(" ADD");

    boolean first = true;
    for (final SinkRecordField f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(lineSeparator);
      builder.append(escapeColumnNamesStart + f.getName() + escapeColumnNamesEnd);
      builder.append(" ");
      builder.append(getSqlType(f.getType()));
      builder.append(" NULL");
    }
    //builder.append(";");
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
    builder.append(" with (HOLDLOCK) AS target using (select ? ");
    builder.append(select);
    builder.append(") AS incoming on(");
    builder.append(String.format("target.%s=incoming.%s", keyColumns.get(0), keyColumns.get(0)));
    for (int i = 1; i < keyColumns.size(); ++i) {
      builder.append(String.format(" and target.%s=incoming.%s", keyColumns.get(i), keyColumns.get(i)));
    }
    builder.append(")");
    if (columns != null && columns.size() > 0) {
      builder.append(" when matched then update set ");
      builder.append(String.format("target.%s=incoming.%s", columns.get(0), columns.get(0)));
      for (int i = 1; i < columns.size(); ++i) {
        builder.append(String.format(",target.%s=incoming.%s", columns.get(i), columns.get(i)));
      }
    }

    final String insertColumns = Joiner.on(",target.").join(iter);
    final String insertValues = Joiner.on(",incoming.").join(iter);

    builder.append(" when not matched then insert(target.");
    builder.append(insertColumns);
    builder.append(") values(incoming.");
    builder.append(insertValues);
    builder.append(")");
    return builder.toString();

  }

  @Override
  public String getMergeHints() {
    return " with (HOLDLOCK)";
  }
}