/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

package io.aiven.connect.jdbc.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.connect.jdbc.dialect.DatabaseDialects;

public class DatabaseDialectRecommender implements ConfigDef.Recommender, ConfigDef.Validator {

    public static final DatabaseDialectRecommender INSTANCE = new DatabaseDialectRecommender();

    private static final List<Object> DIALECT_NAMES;

    static {
        DIALECT_NAMES = new ArrayList<>();
        DIALECT_NAMES.add("");
        DIALECT_NAMES.addAll(DatabaseDialects.registeredDialectNames());
    }

    public List<Object> validValues(final String var1, final Map<String, Object> var2) {
        return DIALECT_NAMES;
    }

    public boolean visible(final String var1, final Map<String, Object> var2) {
        return true;
    }

    @Override
    public void ensureValid(final String key, final Object value) {
        if (value != null && !DIALECT_NAMES.contains(value.toString())) {
            throw new ConfigException(key, value, "Invalid enumerator");
        }
    }

    @Override
    public String toString() {
        return DIALECT_NAMES.toString();
    }
}
