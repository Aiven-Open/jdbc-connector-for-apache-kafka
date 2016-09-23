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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public class SqliteDialect extends DbDialect {
  public SqliteDialect() {
    super("`", "`");
  }

  @Override
  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {
    switch (type) {
      case BOOLEAN:
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return "NUMERIC";
      case FLOAT32:
      case FLOAT64:
        return "REAL";
      case STRING:
        return "TEXT";
      case BYTES:
        return "BLOB";
    }
    return super.getSqlType(schemaName, parameters, type);
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (SinkRecordField field : fields) {
      queries.addAll(super.getAlterTable(tableName, Collections.singleton(field)));
    }
    return queries;
  }

  @Override
  public String getUpsertQuery(String table, Collection<String> keyCols, Collection<String> cols) {
    StringBuilder builder = new StringBuilder();
    builder.append("INSERT OR REPLACE INTO ");
    builder.append(escaped(table)).append("(");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") VALUES(");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(")");
    return builder.toString();
  }
}