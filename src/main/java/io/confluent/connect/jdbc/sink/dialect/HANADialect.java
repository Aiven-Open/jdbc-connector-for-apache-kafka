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

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.Schema;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public class HANADialect extends DbDialect {


  public HANADialect() {
    super(getSqlTypeMap(), getLogicalToSqlTypeMap(), "\"", "\"");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "TINYINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INTEGER");
    map.put(Schema.Type.INT64, "BIGINT");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "DOUBLE");
    map.put(Schema.Type.BOOLEAN, "BOOLEAN");
    map.put(Schema.Type.STRING, "VARCHAR(1000)");
    map.put(Schema.Type.BYTES, "BLOB");
    return map;
  }

  private static Map<String, String> getLogicalToSqlTypeMap() {
    Map<String, String> map = new HashMap<>();
    return map;
  }

  @Override
  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    // Defaulting to Column Store
    return super.getCreateQuery(tableName, fields).replace("CREATE TABLE", "CREATE COLUMN TABLE");
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(escaped(tableName));
    builder.append(" ADD(");
    writeColumnsSpec(builder, fields);
    builder.append(")");
    return Collections.singletonList(builder.toString());
  }

  @Override
  public String getUpsertQuery(final String table, Collection<String> keyCols, Collection<String> cols) {
    // https://help.sap.com/hana_one/html/sql_replace_upsert.html
    StringBuilder builder = new StringBuilder("UPSERT ");
    builder.append(escaped(table));
    builder.append("(");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") VALUES(");
    nCopiesToBuilder(builder, ",", "?", keyCols.size() + cols.size());
    builder.append(")");
    builder.append(" WITH PRIMARY KEY");
    return builder.toString();
  }
}
