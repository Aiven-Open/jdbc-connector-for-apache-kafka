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

package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.datamountaineer.streamreactor.connect.jdbc.sink.Field;
import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import org.apache.kafka.connect.data.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides support for Oracle database
 */
public class OracleDialect extends Sql2003Dialect {
  public OracleDialect() {
    super(getSqlTypeMap());
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, " TINYINT");
    map.put(Schema.Type.INT16, " SMALLINT");
    map.put(Schema.Type.INT32, "INTEGER");
    map.put(Schema.Type.INT64, "BIGINT");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "DOUBLE");
    map.put(Schema.Type.BOOLEAN, "BOOLEAN");
    map.put(Schema.Type.STRING, "VARCHAR(1024)");
    map.put(Schema.Type.BYTES, "BLOB");
    return map;
  }

  @Override
  public String getAlterTable(String table, Collection<Field> fields) {
    ParameterValidator.notNullOrEmpty(table, "table");
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is empty.");
    }
    final StringBuilder builder = new StringBuilder("ALTER TABLE ADD (");
    builder.append(table);
    builder.append(System.lineSeparator());

    boolean first = true;
    for (final Field f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(System.lineSeparator());
      builder.append(" ");
      builder.append(f.getName());
      builder.append(" NULL ");
      builder.append(getSqlType(f.getType()));
    }
    builder.append(");");
    return builder.toString();
  }
}
