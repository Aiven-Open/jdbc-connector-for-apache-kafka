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
