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
import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public class PostgreSqlDialect extends DbDialect {

  // The user is responsible for escaping the columns otherwise create table A and create table "A" is not the same

  public PostgreSqlDialect() {
    super(getSqlTypeMap(), "\"", "\"");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "SMALLINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INT");
    map.put(Schema.Type.INT64, "BIGINT");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "DOUBLE PRECISION");
    map.put(Schema.Type.BOOLEAN, "BOOLEAN");
    map.put(Schema.Type.STRING, "TEXT");
    map.put(Schema.Type.BYTES, "BYTEA");
    return map;
  }

  @Override
  public String getUpsertQuery(final String table, final Collection<String> keyCols, final Collection<String> cols) {
    final StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO ");
    builder.append(escaped(table));
    builder.append(" (");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") VALUES (");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(") ON CONFLICT (");
    joinToBuilder(builder, ",", keyCols, escaper());
    builder.append(") DO UPDATE SET ");
    joinToBuilder(
        builder,
        ",",
        cols,
        new StringBuilderUtil.Transform<String>() {
          @Override
          public void apply(StringBuilder builder, String col) {
            builder.append(escaped(col)).append("=EXCLUDED.").append(escaped(col));
          }
        }
    );
    return builder.toString();
  }
}
