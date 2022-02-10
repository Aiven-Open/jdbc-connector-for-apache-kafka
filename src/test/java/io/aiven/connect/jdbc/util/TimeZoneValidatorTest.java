/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2017 Confluent Inc.
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

import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;

import org.junit.Test;

public class TimeZoneValidatorTest {

    @Test
    public void testAccuracy() {
        final String[] validTimeZones = new String[]{
            "Europe/Vienna",
            "Asia/Tokyo",
            "America/Los_Angeles",
            "UTC",
            "GMT+01:00",
            "UTC"
        };

        Stream.of(validTimeZones)
            .forEach(timeZone -> TimeZoneValidator.INSTANCE.ensureValid("db.timezone", timeZone));
    }

    @Test
    public void testTimeZoneNotSpecified() {
        TimeZoneValidator.INSTANCE.ensureValid("db.timezone", null);
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTimeZone() {
        TimeZoneValidator.INSTANCE.ensureValid("db.timezone", "invalid");
    }

    @Test(expected = ConfigException.class)
    public void testEmptyTimeZone() {
        TimeZoneValidator.INSTANCE.ensureValid("db.timezone", "");
    }
}
