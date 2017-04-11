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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

public class VerticaDialect extends DbDialect {
  public VerticaDialect() {
    super("\"", "\"");
  }

  @Override
  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL(18," + parameters.get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
      }
    }
    switch (type) {
      case INT8:
        return "INT";
      case INT16:
        return "INT";
      case INT32:
        return "INT";
      case INT64:
        return "INT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "FLOAT";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR(1024)";
      case BYTES:
        return "VARBINARY(1024)";
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
}
