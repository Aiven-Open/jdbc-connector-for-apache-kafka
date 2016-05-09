/**
 * Copyright 2015 Datamountaineer.
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
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.google.common.base.Joiner;
import io.confluent.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains the SinkConnect payload fields to consider and/or their mappings
 */
public final class PayloadFields {

  private final Boolean includeAllFields;
  private final Map<String, FieldAlias> fieldsMappings;

  public PayloadFields(final Boolean includeAllFields, final Map<String, FieldAlias> fieldsMappings) {
    this.includeAllFields = includeAllFields;
    this.fieldsMappings = fieldsMappings;
  }

  private PayloadFields() {
    this(true, new HashMap<String, FieldAlias>());
  }

  public Boolean getIncludeAllFields() {
    return includeAllFields;
  }

  public Map<String, FieldAlias> getFieldsMappings() {
    return fieldsMappings;
  }

  public boolean hasPrimaryKeys() {
    for (Map.Entry<String, FieldAlias> e:fieldsMappings.entrySet()) {
      if (e.getValue().isPrimaryKey()) {
        return true;
      }
    }
    return  false;
  }

  @Override
  public String toString() {
    Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
    return String.format("PayloadFields(%s,%s)", includeAllFields.toString(), mapJoiner.join(fieldsMappings));
  }

  public static PayloadFields from(String value) {
    if (value == null) {
      return new PayloadFields();
    }

    final Map<String, FieldAlias> fieldAlias = new HashMap<>();
    for (String split : value.split(",")) {
      final String[] arr = split.trim().split("=");
      if (arr[0].trim().length() == 0) {
        throw new ConfigException("Invalid configuration for fields and mappings. Need to define the field name");
      }

      boolean isPk = isPrimaryKey(arr[0].trim());
      String field;
      if (isPk) {
        field = removePrimaryKeyChars(arr[0].trim());
      } else {
        field = arr[0].trim();
      }

      if (arr.length == 1) {
        fieldAlias.put(field, new FieldAlias(field));
      } else if (arr.length == 2) {
        fieldAlias.put(field, new FieldAlias(arr[1].trim(), isPk));
      } else {
        throw new ConfigException(value + " is not valid. Need to set the fields and mappings like: field1,field2,field3=alias3,[field4, field5=alias5]");
      }
    }

    final Boolean allFields = fieldAlias.remove("*") != null;

    return new PayloadFields(allFields, fieldAlias);
  }

  /**
   * Validates if the incoming field specified in the configuration is a primary key one.If the field is encapsulated
   * between '[]' then is considered to be a primary key field.
   * @param field - The field specified in the configuration
   * @return - true if the field specified is supposed to be a primary key one; false-otherwise
   */
  private static boolean isPrimaryKey(final String field) {
    return field.length() >= 2 && field.charAt(0) == '[' && field.charAt(field.length() - 1) == ']';
  }

  private static String removePrimaryKeyChars(final String field) {
    final String f = field.substring(1, field.length() - 1).trim();
    if (f.length() == 0) {
      throw new ConfigException("Invlaid configuration for field mappings. Missing the primary key field.");
    }
    return f;
  }
}
