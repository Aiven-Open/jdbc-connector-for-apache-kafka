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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;

public class SqlServerDialect extends DbDialect {

  public SqlServerDialect() {
    super("[", "]");
  }

  @Override
  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          return "decimal(38," + parameters.get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "date";
        case Time.LOGICAL_NAME:
          return "time";
        case Timestamp.LOGICAL_NAME:
          return "datetime2";
      }
    }
    switch (type) {
      case INT8:
        return "tinyint";
      case INT16:
        return "smallint";
      case INT32:
        return "int";
      case INT64:
        return "bigint";
      case FLOAT32:
        return "real";
      case FLOAT64:
        return "float";
      case BOOLEAN:
        return "bit";
      case STRING:
        return "varchar(max)";
      case BYTES:
        return "varbinary(max)";
    }
    return super.getSqlType(schemaName, parameters, type);
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
    joinToBuilder(builder, ", ", keyCols, cols, prefixedEscaper("? AS "));
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