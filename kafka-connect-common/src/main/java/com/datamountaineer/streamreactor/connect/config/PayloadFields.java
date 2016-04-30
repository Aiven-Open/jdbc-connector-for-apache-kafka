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
