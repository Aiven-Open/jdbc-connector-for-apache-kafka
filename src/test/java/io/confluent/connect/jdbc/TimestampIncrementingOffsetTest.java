/**
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
 **/

package io.confluent.connect.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class TimestampIncrementingOffsetTest {
  private TimestampIncrementingOffset unset;
  private TimestampIncrementingOffset tsOnly;
  private TimestampIncrementingOffset incOnly;
  private TimestampIncrementingOffset tsInc;
  private TimestampIncrementingOffset nanosOffset;
  private Timestamp ts;
  private Timestamp nanos;
  private long id;

  @Before
  public void setUp() {
    ts = new Timestamp(100L);
    id = 1000L;
    unset = new TimestampIncrementingOffset();
    tsOnly = new TimestampIncrementingOffset(ts, null);
    incOnly = new TimestampIncrementingOffset(null, id);
    tsInc = new TimestampIncrementingOffset(ts, id);

    long millis = System.currentTimeMillis();
    nanos = new Timestamp(millis);
    nanos.setNanos((int)(millis % 1000) * 1000000 + 123456);
    assertEquals(millis, nanos.getTime());
    nanosOffset = new TimestampIncrementingOffset(nanos, null);
  }

  @Test
  public void testDefaults() {
    assertEquals(-1, unset.getIncrementingOffset());
    assertNotNull(unset.getTimestampOffset());
    assertEquals(0, unset.getTimestampOffset().getTime());
    assertEquals(0, unset.getTimestampOffset().getNanos());
  }

  @Test
  public void testToMap() {
    assertEquals(0, unset.toMap().size());
    assertEquals(2, tsOnly.toMap().size());
    assertEquals(1, incOnly.toMap().size());
    assertEquals(3, tsInc.toMap().size());
    assertEquals(2, nanosOffset.toMap().size());
  }

  @Test
  public void testGetIncrementingOffset() {
    assertEquals(-1, unset.getIncrementingOffset());
    assertEquals(-1, tsOnly.getIncrementingOffset());
    assertEquals(id, incOnly.getIncrementingOffset());
    assertEquals(id, tsInc.getIncrementingOffset());
    assertEquals(-1, nanosOffset.getIncrementingOffset());
  }

  @Test
  public void testGetTimestampOffset() {
    assertNotNull(unset.getTimestampOffset());
    Timestamp zero = new Timestamp(0);
    assertEquals(zero, unset.getTimestampOffset());
    assertEquals(ts, tsOnly.getTimestampOffset());
    assertEquals(zero, incOnly.getTimestampOffset());
    assertEquals(ts, tsInc.getTimestampOffset());
    assertEquals(nanos, nanosOffset.getTimestampOffset());
  }

  @Test
  public void testSetTimestampOffset() {
    assertNotEquals(unset, tsOnly);
    assertEquals(ts, tsOnly.getTimestampOffset());
    tsOnly.setTimestampOffset(null);
    assertNotNull(tsOnly.getTimestampOffset());
    assertEquals(new Timestamp(0), tsOnly.getTimestampOffset());
    assertEquals(unset, tsOnly);
    tsInc.setTimestampOffset(null);
    assertNotNull(tsInc.getTimestampOffset());
    assertEquals(new Timestamp(0), tsInc.getTimestampOffset());
    assertEquals(incOnly, tsInc);
  }

  @Test
  public void testSetIncrementingOffset() {
    assertNotEquals(unset, incOnly);
    assertEquals(id, incOnly.getIncrementingOffset());
    incOnly.setIncrementingOffset(null);
    assertNotNull(incOnly.getIncrementingOffset());
    assertEquals(-1, incOnly.getIncrementingOffset());
    assertEquals(unset, incOnly);
    tsInc.setIncrementingOffset(null);
    assertNotNull(tsInc.getIncrementingOffset());
    assertEquals(-1, tsInc.getIncrementingOffset());
    assertEquals(tsOnly, tsInc);
  }

  @Test
  public void testFromMap() {
    assertEquals(unset, TimestampIncrementingOffset.fromMap(unset.toMap()));
    assertEquals(tsOnly, TimestampIncrementingOffset.fromMap(tsOnly.toMap()));
    assertEquals(incOnly, TimestampIncrementingOffset.fromMap(incOnly.toMap()));
    assertEquals(tsInc, TimestampIncrementingOffset.fromMap(tsInc.toMap()));
    assertEquals(nanosOffset, TimestampIncrementingOffset.fromMap(nanosOffset.toMap()));
  }

  @Test
  public void testEquals() {
    assertEquals(nanosOffset, nanosOffset);
    assertEquals(new TimestampIncrementingOffset(), new TimestampIncrementingOffset());
    assertEquals(new TimestampIncrementingOffset(), new TimestampIncrementingOffset(null, null));
    assertEquals(unset, new TimestampIncrementingOffset());
    assertEquals(unset, new TimestampIncrementingOffset(null, null));

    TimestampIncrementingOffset x = new TimestampIncrementingOffset(null, id);
    assertEquals(x, incOnly);

    x = new TimestampIncrementingOffset(ts, null);
    assertEquals(x, tsOnly);

    x = new TimestampIncrementingOffset(ts, id);
    assertEquals(x, tsInc);

    x = new TimestampIncrementingOffset(nanos, null);
    assertEquals(x, nanosOffset);
  }

}
