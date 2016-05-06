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

package com.datamountaineer.streamreactor.connect.config;

import com.google.common.base.Joiner;
import io.confluent.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains the SinkConnect payload fields to consider and/or their mappings
 */
public final class PayloadFields {

    private final Boolean includeAllFields;
    private final Map<String, String> fieldsMappings;

    public PayloadFields(Boolean includeAllFields, Map<String, String> fieldsMappings) {
        this.includeAllFields = includeAllFields;
        this.fieldsMappings = fieldsMappings;
    }

    public PayloadFields() {
        this(true, new HashMap<String, String>());
    }

    public Boolean getIncludeAllFields() {
        return includeAllFields;
    }

    public Map<String, String> getFieldsMappings() {
        return fieldsMappings;
    }

    public static PayloadFields from(String value) {
        if (value == null)
            return new PayloadFields();

        final Map<String, String> fieldAlias = new HashMap<>();
        for (String split : value.split(",")) {
            final String[] arr = split.trim().split("=");
            if (arr.length == 1) {
                fieldAlias.put(arr[0], arr[0]);
            } else if (arr.length == 2) {
                fieldAlias.put(arr[0], arr[1]);
            } else
                throw new ConfigException(value + " is not valid. Need to set the fields and mappings like: field1,field2,field3=alias3,[field4, field5=alias5]");
        }

        final Boolean allFields = fieldAlias.remove("*") != null;

        return new PayloadFields(allFields, fieldAlias);
    }

    @Override
    public String toString() {
        Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
        return String.format("PayloadFields(%s,%s)", includeAllFields.toString(), mapJoiner.join(fieldsMappings));
    }
}
