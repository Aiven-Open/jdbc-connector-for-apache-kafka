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

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;

/**
 * Contains the SinkRecord field and schema. It is used in conjunction with table schema evolution
 */
public class SinkRecordField {
  private final boolean isPrimaryKey;
  private final Schema.Type type;
  private final String name;

  public SinkRecordField(final Schema.Type type, final String name, final  boolean isPrimaryKey) {
    this.type = type;
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
  }

  public Schema.Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }
}
