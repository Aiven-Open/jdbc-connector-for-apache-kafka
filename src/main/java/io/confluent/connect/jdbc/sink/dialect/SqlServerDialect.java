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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;

public class SqlServerDialect extends DbDialect {

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
    map.put(Schema.Type.STRING, "varchar(max)");
    map.put(Schema.Type.BYTES, "varbinary(max)");
    return map;
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(escaped(tableName));
    builder.append(" ADD");
    writeColumnsSpec(builder, fields);
    return Collections.singletonList(builder.toString());
  }

  @Override
  public String getUpsertQuery(String table, Collection<String> keyCols, Collection<String> cols) {
    final StringBuilder builder = new StringBuilder();
    builder.append("merge into ");
    String tableName = escaped(table);
    builder.append(tableName);
    builder.append(" with (HOLDLOCK) AS target using (select ");
    joinToBuilder(builder, ", ", cols, keyCols, prefixedEscaper("? AS "));
    builder.append(") AS incoming on (");
    joinToBuilder(builder, " and ", keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append("target.").append(escaped(col)).append("=incoming.").append(escaped(col));
      }
    });
    builder.append(")");
    if (cols != null && cols.size() > 0) {
      builder.append(" when matched then update set ");
      joinToBuilder(builder, ",", cols, new StringBuilderUtil.Transform<String>() {
        @Override
        public void apply(StringBuilder builder, String col) {
          builder.append(escaped(col)).append("=incoming.").append(escaped(col));
        }
      });
    }
    builder.append(" when not matched then insert (");
    joinToBuilder(builder, ", ", cols, keyCols, escaper());
    builder.append(") values (");
    joinToBuilder(builder, ",", cols, keyCols, prefixedEscaper("incoming."));
    builder.append(");");
    return builder.toString();
  }
}