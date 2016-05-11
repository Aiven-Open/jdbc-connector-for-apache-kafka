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

package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.google.common.base.Joiner;
import io.confluent.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains the SinkConnect payload fields to consider and/or their mappings
 */
public final class FieldsMappings {
  private final String tableName;
  private final String incomingTopic;
  private final Boolean allFieldsIncluded;
  private final Map<String, FieldAlias> mappings;

  /**
   * Creates a new instance of FieldsMappings
   *
   * @param tableName         - The target RDBMS table to insert the records into
   * @param incomingTopic     - The source Kafka topic
   * @param allFieldsIncluded - If set to true it considers all fields in the payload; if false it will rely on the
   *                          defined fields to include
   * @param mappings          - Provides the map of fields to include and their alias. It could be set to Map.empty if all fields
   *                          are to be included.
   */
  public FieldsMappings(final String tableName,
                        final String incomingTopic,
                        final Boolean allFieldsIncluded,
                        final Map<String, FieldAlias> mappings) {
    ParameterValidator.notNullOrEmpty(tableName, "tableName");
    ParameterValidator.notNullOrEmpty(incomingTopic, "incomingTopic");
    ParameterValidator.notNull(mappings, "map");

    this.tableName = tableName;
    this.incomingTopic = incomingTopic;
    this.allFieldsIncluded = allFieldsIncluded;
    this.mappings = mappings;
  }

  public FieldsMappings(final String tableName, final String incomingTopic) {
    this(tableName, incomingTopic, true, new HashMap<String, FieldAlias>());
  }

  /**
   * If set to true all the incoming SinkRecord payload fields are considered for inserting into the table.
   *
   * @return true - if all payload fields should be included; false - otherwise
   */
  public Boolean areAllFieldsIncluded() {
    return allFieldsIncluded;
  }

  public Map<String, FieldAlias> getMappings() {
    return mappings;
  }

  /**
   * Returns the source Kafka topic.
   * @return
   */
  public String getIncomingTopic() {
    return incomingTopic;
  }

  /**
   * Returns the target database table name.
   * @return
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns true if any of the filed mappings provided are part of the table primary key.
   *
   * @return
   */
  public boolean hasPrimaryKeys() {
    for (Map.Entry<String, FieldAlias> e : mappings.entrySet()) {
      if (e.getValue().isPrimaryKey())
        return true;
    }
    return false;
  }


  @Override
  public String toString() {
    Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
    return String.format("PayloadFields(%s,%s)", allFieldsIncluded.toString(), mapJoiner.join(mappings));
  }

  public static FieldsMappings from(final String tableName, final String incomingTopic, final String value) {
    if (value == null)
      return new FieldsMappings(tableName, incomingTopic);

    final Map<String, FieldAlias> fieldAlias = new HashMap<>();
    for (String split : value.split(",")) {
      final String[] arr = split.trim().split("=");
      if (arr[0].trim().length() == 0)
        throw new ConfigException("Invalid configuration for fields and mappings. Need to define the field name");

      boolean isPk = isPrimaryKey(arr[0].trim());
      String field;
      if (isPk) field = removePrimaryKeyChars(arr[0].trim());
      else field = arr[0].trim();

      if (arr.length == 1) {
        fieldAlias.put(field, new FieldAlias(field));
      } else if (arr.length == 2) {
        fieldAlias.put(field, new FieldAlias(arr[1].trim(), isPk));
      } else
        throw new ConfigException(value + " is not valid. Need to set the fields and mappings like: field1,field2,field3=alias3,[field4, field5=alias5]");
    }

    final Boolean allFields = fieldAlias.remove("*") != null;

    return new FieldsMappings(tableName, incomingTopic, allFields, fieldAlias);
  }

  /**
   * Validates if the incoming field specified in the configuration is a primary key one.If the field is encapsulated
   * between '[]' then is considered to be a primary key field.
   *
   * @param field - The field specified in the configuration
   * @return - true if the field specified is supposed to be a primary key one; false-otherwise
   */
  public static boolean isPrimaryKey(final String field) {
    boolean isPk = field.length() >= 2 && field.charAt(0) == '[' && field.charAt(field.length() - 1) == ']';
    if (isPk) {
      if (field.substring(1, field.length() - 1).isEmpty())
        throw new ConfigException("Invalid configuration for field mappings.The primary key is not named.");
    }
    return isPk;
  }

  public static String removePrimaryKeyChars(final String field) {
    final String f = field.substring(1, field.length() - 1).trim();
    if (f.length() == 0) {
      throw new ConfigException("Invlaid configuration for field mappings. Missing the primary key field.");
    }
    return f;
  }
}
