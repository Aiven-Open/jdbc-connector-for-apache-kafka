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

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class TimestampIncrementingOffset {
  private Long incrementingOffset;
  private Timestamp timestampOffset;

  public TimestampIncrementingOffset() {}

  public TimestampIncrementingOffset(Timestamp ts, Long id) {
    this.timestampOffset = ts;
    this.incrementingOffset = id;
  }

  public long getIncrementingOffset() {
    return incrementingOffset == null ? -1 : incrementingOffset;
  }

  public void setIncrementingOffset(Long incrementingOffset) {
    if (incrementingOffset != null && incrementingOffset < -1)
      throw new IllegalArgumentException("incrementingOffset must be -1 or non-negative");
    this.incrementingOffset = incrementingOffset;
  }

  public Timestamp getTimestampOffset() {
    return timestampOffset == null ? new Timestamp(0) : timestampOffset;
  }

  public void setTimestampOffset(Timestamp timestampOffset) {
    this.timestampOffset = timestampOffset;
  }

  private static final String INCREMENTING_FIELD = "incrementing";
  private static final String TIMESTAMP_MILLIS_FIELD = "timestamp_millis";
  private static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    if (incrementingOffset != null)
      map.put(INCREMENTING_FIELD, incrementingOffset);
    if (timestampOffset != null) {
      map.put(TIMESTAMP_MILLIS_FIELD, timestampOffset.getTime());
      map.put(TIMESTAMP_NANOS_FIELD, timestampOffset.getNanos());
    }
    return map;
  }

  public static TimestampIncrementingOffset fromMap(Map<String, ?> map) {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset();
    if (map == null || map.isEmpty())
      return offset;
    Object incr = map.get(INCREMENTING_FIELD);
    if (incr != null)
      offset.setIncrementingOffset((Long) incr);
    Object millis = map.get(TIMESTAMP_MILLIS_FIELD);
    if (millis != null) {
      Timestamp ts = new Timestamp((Long) millis);
      ts.setNanos((Integer) map.get(TIMESTAMP_NANOS_FIELD));
      offset.setTimestampOffset(ts);
    }
    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TimestampIncrementingOffset that = (TimestampIncrementingOffset) o;

    if (incrementingOffset != null ? !incrementingOffset.equals(that.incrementingOffset) : that.incrementingOffset != null)
      return false;
    return timestampOffset != null ? timestampOffset.equals(that.timestampOffset) : that.timestampOffset == null;

  }

  @Override
  public int hashCode() {
    int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
    result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
    return result;
  }
}
