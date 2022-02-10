/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2015 Confluent Inc.
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

package io.aiven.connect.jdbc.util;

import java.time.DateTimeException;
import java.time.ZoneId;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class TimeZoneValidator implements ConfigDef.Validator {

    public static final TimeZoneValidator INSTANCE = new TimeZoneValidator();

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null) {
            try {
                ZoneId.of(value.toString());
            } catch (final DateTimeException e) {
                throw new ConfigException(name, value, "Invalid time zone identifier");
            }
        }
    }

    @Override
    public String toString() {
        return "valid time zone identifier (e.g., 'Europe/Helsinki', 'UTC+2', 'Z', 'CET')";
    }
}
