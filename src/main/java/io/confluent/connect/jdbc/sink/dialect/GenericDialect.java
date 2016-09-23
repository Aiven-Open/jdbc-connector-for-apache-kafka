/*
 *  Copyright 2016 Confluent Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.confluent.connect.jdbc.sink.dialect;

import java.util.Collection;
import java.util.List;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

public class GenericDialect extends DbDialect {
  public GenericDialect() {
    super("\"", "\"");
  }

  // Only INSERT supported for now. CREATE and ALTER may be possible if we can figure out a reasonable type map.

  @Override
  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getUpsertQuery(String table, Collection<String> keyColumns, Collection<String> columns) {
    throw new UnsupportedOperationException();
  }
}
