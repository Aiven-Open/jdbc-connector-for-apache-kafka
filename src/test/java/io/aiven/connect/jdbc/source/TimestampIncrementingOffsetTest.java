/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

package io.aiven.connect.jdbc.source;

import java.sql.Timestamp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TimestampIncrementingOffsetTest {
    private final Timestamp ts = new Timestamp(100L);
    private final long id = 1000L;
    private final TimestampIncrementingOffset unset = new TimestampIncrementingOffset(null, null);
    private final TimestampIncrementingOffset tsOnly = new TimestampIncrementingOffset(ts, null);
    private final TimestampIncrementingOffset incOnly = new TimestampIncrementingOffset(null, id);
    private final TimestampIncrementingOffset tsInc = new TimestampIncrementingOffset(ts, id);
    private Timestamp nanos;
    private TimestampIncrementingOffset nanosOffset;

    @BeforeEach
    public void setUp() {
        final long millis = System.currentTimeMillis();
        nanos = new Timestamp(millis);
        nanos.setNanos((int) (millis % 1000) * 1000000 + 123456);
        assertThat(nanos.getTime()).isEqualTo(millis);
        nanosOffset = new TimestampIncrementingOffset(nanos, null);
    }

    @Test
    public void testDefaults() {
        assertThat(unset.getIncrementingOffset()).isNull();
        assertThat(unset.getTimestampOffset()).isNull();
    }

    @Test
    public void testToMap() {
        assertThat(unset.toMap()).isEmpty();
        assertThat(tsOnly.toMap()).hasSize(2);
        assertThat(incOnly.toMap()).hasSize(1);
        assertThat(tsInc.toMap()).hasSize(3);
        assertThat(nanosOffset.toMap()).hasSize(2);
    }

    @Test
    public void testGetIncrementingOffset() {
        assertThat(unset.getIncrementingOffset()).isNull();
        assertThat(tsOnly.getIncrementingOffset()).isNull();
        assertThat(incOnly.getIncrementingOffset().longValue()).isEqualTo(id);
        assertThat(tsInc.getIncrementingOffset()).isEqualTo(id);
        assertThat(nanosOffset.getIncrementingOffset()).isNull();
    }

    @Test
    public void testGetTimestampOffset() {
        assertThat(unset.getTimestampOffset()).isNull();
        assertThat(tsOnly.getTimestampOffset()).isEqualTo(ts);
        assertThat(tsOnly.getTimestampOffset()).isEqualTo(ts);
        assertThat(incOnly.getTimestampOffset()).isNull();
        assertThat(tsInc.getTimestampOffset()).isEqualTo(ts);
        assertThat(nanosOffset.getTimestampOffset()).isEqualTo(nanos);
    }

    @Test
    public void testFromMap() {
        assertThat(TimestampIncrementingOffset.fromMap(unset.toMap())).isEqualTo(unset);
        assertThat(TimestampIncrementingOffset.fromMap(tsOnly.toMap())).isEqualTo(tsOnly);
        assertThat(TimestampIncrementingOffset.fromMap(incOnly.toMap())).isEqualTo(incOnly);
        assertThat(TimestampIncrementingOffset.fromMap(tsInc.toMap())).isEqualTo(tsInc);
        assertThat(TimestampIncrementingOffset.fromMap(nanosOffset.toMap())).isEqualTo(nanosOffset);
    }

    @Test
    public void testEquals() {
        assertThat(nanosOffset).isEqualTo(nanosOffset);
        assertThat(new TimestampIncrementingOffset(null, null)).isEqualTo(new TimestampIncrementingOffset(null, null));
        assertThat(new TimestampIncrementingOffset(null, null)).isEqualTo(unset);

        TimestampIncrementingOffset x = new TimestampIncrementingOffset(null, id);
        assertThat(incOnly).isEqualTo(x);

        x = new TimestampIncrementingOffset(ts, null);
        assertThat(tsOnly).isEqualTo(x);

        x = new TimestampIncrementingOffset(ts, id);
        assertThat(tsInc).isEqualTo(x);

        x = new TimestampIncrementingOffset(nanos, null);
        assertThat(nanosOffset).isEqualTo(x);
    }

}
