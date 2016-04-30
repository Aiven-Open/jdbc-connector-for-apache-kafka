package com.datamountaineer.streamreactor.connect;

import java.util.HashMap;
import java.util.Map;

public final class FieldNameAndValue {
    private final String name;
    private final Object value;

    public FieldNameAndValue(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public static Map<String, Object> toMap(final Iterable<FieldNameAndValue> it) {
        final HashMap<String, Object> map = new HashMap<>();
        for (FieldNameAndValue nv : it) {
            map.put(nv.getName(), nv.getValue());
        }
        return map;
    }
}
