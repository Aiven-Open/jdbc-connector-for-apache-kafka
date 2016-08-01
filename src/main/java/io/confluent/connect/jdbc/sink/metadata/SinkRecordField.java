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

package io.confluent.connect.jdbc.sink.metadata;

import org.apache.kafka.connect.data.Schema;

public class SinkRecordField {
  public final Schema.Type type;
  public final String name;
  public final boolean isPrimaryKey;
  public final boolean isOptional;
  public final Object defaultValue;

  public SinkRecordField(final Schema.Type type, final String name, final boolean isPrimaryKey) {
    this(type, name, isPrimaryKey, !isPrimaryKey, null);
  }

  public SinkRecordField(
      final Schema.Type type,
      final String name,
      final boolean isPrimaryKey,
      final boolean isOptional,
      final Object defaultValue
  ) {
    this.type = type;
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
    this.isOptional = isOptional;
    this.defaultValue = defaultValue;
  }

  @Override
  public String toString() {
    return "SinkRecordField{" +
           "type=" + type +
           ", name='" + name + '\'' +
           ", isPrimaryKey=" + isPrimaryKey +
           ", isOptional=" + isOptional +
           '}';
  }
}
