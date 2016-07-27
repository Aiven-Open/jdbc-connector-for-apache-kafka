/*
 * Copyright 2016 Confluent Inc.
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
 */

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.stringSurroundTransform;

public class SqliteDialect extends DbDialect {
  public SqliteDialect() {
    super(getSqlTypeMap(), "`", "`");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "NUMERIC");
    map.put(Schema.Type.INT16, "NUMERIC");
    map.put(Schema.Type.INT32, "NUMERIC");
    map.put(Schema.Type.INT64, "NUMERIC");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "REAL");
    map.put(Schema.Type.BOOLEAN, "NUMERIC");
    map.put(Schema.Type.STRING, "TEXT");
    map.put(Schema.Type.BYTES, "BLOB");
    return map;
  }

  /**
   * Returns the query for creating a new table in the database
   *
   * @return The create query for the dialect
   */
  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    final StringBuilder builder = new StringBuilder();
    builder.append(String.format("CREATE TABLE %s (", escapeTableName(tableName)));

    joinToBuilder(builder, ",", fields, new StringBuilderUtil.Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField f) {
        builder.append(lineSeparator);
        builder.append(escapeColumnNamesStart).append(f.name).append(escapeColumnNamesEnd);
        builder.append(" ");
        builder.append(getSqlType(f.type));
        if (f.isOptional) {
          builder.append(" NULL");
        } else {
          builder.append(" NOT NULL ");
        }
      }
    });

    final List<String> pks = new ArrayList<>();
    for (SinkRecordField f : fields) {
      if (f.isPrimaryKey) {
        pks.add(f.name);
      }
    }

    if (!pks.isEmpty()) {
      builder.append(",");
      builder.append(lineSeparator);
      builder.append("PRIMARY KEY(");
      joinToBuilder(builder, ",", pks, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
      builder.append(")");
    }

    builder.append(");");
    return builder.toString();
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (final SinkRecordField f : fields) {
      queries.add(String.format(
          "ALTER TABLE %s ADD %s%s%s %s %s",
          escapeTableName(tableName),
          escapeColumnNamesStart, f.name, escapeColumnNamesEnd,
          getSqlType(f.type),
          f.isOptional ? "NULL" : "NOT NULL"
      ));
    }
    return queries;
  }

  @Override
  public String getUpsertQuery(String table, Collection<String> keyCols, Collection<String> cols) {
    StringBuilder builder = new StringBuilder();
    builder.append("INSERT OR REPLACE INTO ");
    builder.append(escapeTableName(table)).append("(");
    joinToBuilder(builder, ",", keyCols, cols, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") VALUES(");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(")");
    return builder.toString();
  }
}