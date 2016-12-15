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
import java.util.Map;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public class MySqlDialect extends DbDialect {

  public MySqlDialect() {
    super("`", "`");
  }

  @Override
  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          // Maximum precision supported by MySQL is 65
          return "DECIMAL(65," + Integer.parseInt(parameters.get(Decimal.SCALE_FIELD)) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME(3)";
        case Timestamp.LOGICAL_NAME:
          return "DATETIME(3)";
      }
    }
    switch (type) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "TINYINT";
      case STRING:
        return "VARCHAR(256)";
      case BYTES:
        return "VARBINARY(1024)";
    }
    return super.getSqlType(schemaName, parameters, type);
  }

  @Override
  public String getUpsertQuery(final String table, final Collection<String> keyCols, final Collection<String> cols) {
    //MySql doesn't support SQL 2003:merge so here how the upsert is handled

    final StringBuilder builder = new StringBuilder();
    builder.append("insert into ");
    builder.append(escaped(table));
    builder.append("(");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") values(");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(") on duplicate key update ");
    joinToBuilder(
        builder,
        ",",
        cols.isEmpty() ? keyCols : cols,
        new StringBuilderUtil.Transform<String>() {
          @Override
          public void apply(StringBuilder builder, String col) {
            builder.append(escaped(col)).append("=values(").append(escaped(col)).append(")");
          }
        }
    );
    return builder.toString();
  }

}
