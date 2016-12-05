/**
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
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.util.DateTimeUtils;

import static org.junit.Assert.assertEquals;

// Tests of polling that return data updates, i.e. verifies the different behaviors for getting
// incremental data updates from the database
public class JdbcSourceTaskUpdateTest extends JdbcSourceTaskTestBase {
  private static final Map<String, String> QUERY_SOURCE_PARTITION
      = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                 JdbcSourceConnectorConstants.QUERY_NAME_VALUE);

  @After
  public void tearDown() throws Exception {
    task.stop();
    super.tearDown();
  }

  @Test
  public void testBulkPeriodicLoad() throws Exception {
    EmbeddedDerby.ColumnName column = new EmbeddedDerby.ColumnName("id");
    db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);

    // Bulk periodic load is currently the default
    task.start(singleTableConfig());

    List<SourceRecord> records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.insert(SINGLE_TABLE_NAME, "id", 2);
    records = task.poll();
    Map<Integer, Integer> twoRecords = new HashMap<>();
    twoRecords.put(1, 1);
    twoRecords.put(2, 1);
    assertEquals(twoRecords, countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.delete(SINGLE_TABLE_NAME, new EmbeddedDerby.EqualsCondition(column, 1));
    records = task.poll();
    assertEquals(Collections.singletonMap(2, 1), countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);
  }

  @Test(expected = ConnectException.class)
  public void testIncrementingInvalidColumn() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();

    // Incrementing column must be NOT NULL
    db.createTable(SINGLE_TABLE_NAME, "id", "INT");

    startTask(null, "id", null);

    PowerMock.verifyAll();
  }

  @Test(expected = ConnectException.class)
  public void testTimestampInvalidColumn() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();

    // Timestamp column must be NOT NULL
    db.createTable(SINGLE_TABLE_NAME, "modified", "TIMESTAMP");

    startTask("modified", null, null);

    PowerMock.verifyAll();
  }

  @Test
  public void testManualIncrementing() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();

    db.createTable(SINGLE_TABLE_NAME,
            "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);

    startTask(null, "id", null);
    verifyIncrementingFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // Adding records should result in only those records during the next poll()
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 3);

    verifyPoll(2, "id", Arrays.asList(2, 3), false, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testAutoincrement() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();

    String extraColumn = "col";
    // Need extra column to be able to insert anything, extra is ignored.
    db.createTable(SINGLE_TABLE_NAME,
            "id", "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
            extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);

    startTask(null, "", null); // auto-incrementing
    verifyIncrementingFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // Adding records should result in only those records during the next poll()
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    verifyPoll(2, "id", Arrays.asList(2, 3), false, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestamp() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT");
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 1);

    startTask("modified", null, null);
    verifyTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // If there isn't enough resolution, this could miss some rows. In this case, we'll only see
    // IDs 3 & 4.
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(11L)), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(12L)), "id", 4);

    verifyPoll(2, "id", Arrays.asList(3, 4), true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampWithDelay() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");

    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 1);

    startTask("modified", null, null, 4L);
    verifyTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    Long currentTime = new Date().getTime();

    // Validate that we are seeing 2,3 but not 4,5 as they are getting delayed to the next round
    // Using "toString" and not UTC because Derby's current_timestamp is always local time (i.e. doesn't honor Calendar settings)
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime).toString(), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime+1L).toString(), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime+500L).toString(), "id", 4);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime+501L).toString(), "id", 5);

    verifyPoll(2, "id", Arrays.asList(2, 3), true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // make sure we get the rest
    Thread.sleep(500);
    verifyPoll(2, "id", Arrays.asList(4, 5), true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }


  @Test
  public void testTimestampAndIncrementing() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 1);

    startTask("modified", "id", null);
    verifyIncrementingAndTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // Should be able to pick up id 2 because of ID despite same timestamp.
    // Note this is setup so we can reuse some validation code
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(11L)), "id", 1);

    verifyPoll(2, "id", Arrays.asList(3, 1), true, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testManualIncrementingRestoreOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    expectInitialize(Arrays.asList(SINGLE_TABLE_PARTITION),
            Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap()));

    PowerMock.replayAll();

    db.createTable(SINGLE_TABLE_NAME,
            "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 3);

    startTask(null, "id", null);

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(2, 3), false, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testAutoincrementRestoreOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    expectInitialize(Arrays.asList(SINGLE_TABLE_PARTITION),
            Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap()));

    PowerMock.replayAll();

    String extraColumn = "col";
    // Use BIGINT here to test LONG columns
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY",
                   extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    startTask(null, "", null); // autoincrementing

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(2L, 3L), false, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampRestoreOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), null);
    expectInitialize(Arrays.asList(SINGLE_TABLE_PARTITION),
            Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap()));

    PowerMock.replayAll();

    // Timestamp is managed manually here so we can verify handling of duplicate values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");
    // id=2 will be ignored since it has the same timestamp as the initial offset.
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(11L)), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(12L)), "id", 4);

    startTask("modified", null, null);

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(3, 4), true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampAndIncrementingRestoreOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), 3L);
    expectInitialize(Arrays.asList(SINGLE_TABLE_PARTITION),
            Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap()));

    PowerMock.replayAll();

    // Timestamp is managed manually here so we can verify handling of duplicate values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT NOT NULL");
    // id=3 will be ignored since it has the same timestamp + id as the initial offset, rest
    // should be included, including id=1 which is an old ID with newer timestamp
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(9L)), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(11L)), "id", 4);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(12L)), "id", 5);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(13L)), "id", 1);

    startTask("modified", "id", null);

    verifyPoll(3, "id", Arrays.asList(4, 5, 1), true, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testCustomQueryBulk() throws Exception {
    db.createTable(JOIN_TABLE_NAME, "user_id", "INT", "name", "VARCHAR(64)");
    db.insert(JOIN_TABLE_NAME, "user_id", 1, "name", "Alice");
    db.insert(JOIN_TABLE_NAME, "user_id", 2, "name", "Bob");

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT",
                   "user_id", "INT");
    db.insert(SINGLE_TABLE_NAME, "id", 1, "user_id", 1);

    startTask(null, null, "SELECT \"test\".\"id\", \"test\""
                          + ".\"user_id\", \"users\".\"name\" FROM \"test\" JOIN \"users\" "
                          + "ON (\"test\".\"user_id\" = \"users\".\"user_id\")");

    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    Map<Integer, Integer> recordUserIdCounts = new HashMap<>();
    recordUserIdCounts.put(1, 1);
    assertEquals(recordUserIdCounts, countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX);
    assertRecordsSourcePartition(records, QUERY_SOURCE_PARTITION);

    db.insert(SINGLE_TABLE_NAME, "id", 2, "user_id", 1);
    db.insert(SINGLE_TABLE_NAME, "id", 3, "user_id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 4, "user_id", 2);

    records = task.poll();
    assertEquals(4, records.size());
    recordUserIdCounts = new HashMap<>();
    recordUserIdCounts.put(1, 2);
    recordUserIdCounts.put(2, 2);
    assertEquals(recordUserIdCounts, countIntValues(records, "user_id"));
    assertRecordsTopic(records, TOPIC_PREFIX);
    assertRecordsSourcePartition(records, QUERY_SOURCE_PARTITION);
  }

  @Test
  public void testCustomQueryWithTimestamp() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(JOIN_QUERY_PARTITION));

    PowerMock.replayAll();

    db.createTable(JOIN_TABLE_NAME, "user_id", "INT", "name", "VARCHAR(64)");
    db.insert(JOIN_TABLE_NAME, "user_id", 1, "name", "Alice");
    db.insert(JOIN_TABLE_NAME, "user_id", 2, "name", "Bob");

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT",
            "user_id", "INT");
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 1,
              "user_id", 1);

    startTask("modified", null, "SELECT \"test\".\"modified\", \"test\".\"id\", \"test\""
            + ".\"user_id\", \"users\".\"name\" FROM \"test\" JOIN \"users\" "
            + "ON (\"test\".\"user_id\" = \"users\".\"user_id\")");

    verifyTimestampFirstPoll(TOPIC_PREFIX);

    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(10L)), "id", 2,
              "user_id", 1);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(11L)), "id", 3,
              "user_id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatUtcTimestamp(new Timestamp(12L)), "id", 4,
              "user_id", 2);

    verifyPoll(2, "id", Arrays.asList(3, 4), true, false, TOPIC_PREFIX);

    PowerMock.verifyAll();
  }

  private void startTask(String timestampColumn, String incrementingColumn, String query) {
    startTask(timestampColumn, incrementingColumn, query, 0L);
  }

  private void startTask(String timestampColumn, String incrementingColumn, String query, Long delay) {
    String mode = null;
    if (timestampColumn != null && incrementingColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING;
    } else if (timestampColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP;
    } else if (incrementingColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_INCREMENTING;
    } else {
      mode = JdbcSourceConnectorConfig.MODE_BULK;
    }
    initializeTask();
    Map<String, String> taskConfig = singleTableConfig();
    taskConfig.put(JdbcSourceConnectorConfig.MODE_CONFIG, mode);
    if (query != null) {
      taskConfig.put(JdbcSourceTaskConfig.QUERY_CONFIG, query);
      taskConfig.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
    }
    if (timestampColumn != null) {
      taskConfig.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumn);
    }
    if (incrementingColumn != null) {
      taskConfig.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumn);
    }
    taskConfig.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, delay == null ? "0" : delay.toString());
    task.start(taskConfig);
  }

  private void verifyIncrementingFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertEquals(Collections.singletonMap(1L, 1), countIntIncrementingOffsets(records, "id"));
    assertIncrementingOffsets(records);
    assertRecordsTopic(records, topic);
  }

  private List<SourceRecord> verifyTimestampFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertEquals(Collections.singletonMap(10L, 1), countTimestampValues(records, "modified"));
    assertTimestampOffsets(records);
    assertRecordsTopic(records, topic);
    return records;
  }

  private void verifyIncrementingAndTimestampFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = verifyTimestampFirstPoll(topic);
    assertIncrementingOffsets(records);
  }

  private <T> void verifyPoll(int numRecords, String valueField, List<T> values,
                          boolean timestampOffsets, boolean incrementingOffsets, String topic)
      throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(numRecords, records.size());

    HashMap<T, Integer> valueCounts = new HashMap<>();
    for(T value : values) {
      valueCounts.put(value, 1);
    }
    assertEquals(valueCounts, countIntValues(records, valueField));

    if (timestampOffsets) {
      assertTimestampOffsets(records);
    }
    if (incrementingOffsets) {
      assertIncrementingOffsets(records);
    }

    assertRecordsTopic(records, topic);
  }

  private enum Field {
    KEY,
    VALUE,
    TIMESTAMP_VALUE,
    INCREMENTING_OFFSET,
    TIMESTAMP_OFFSET
  }

  private <T> Map<T, Integer> countInts(List<SourceRecord> records, Field field, String fieldName) {
    Map<T, Integer> result = new HashMap<>();
    for (SourceRecord record : records) {
      T extracted;
      switch (field) {
        case KEY:
          extracted = (T)record.key();
          break;
        case VALUE:
          extracted = (T)((Struct)record.value()).get(fieldName);
          break;
        case TIMESTAMP_VALUE: {
          java.util.Date rawTimestamp = (java.util.Date) ((Struct)record.value()).get(fieldName);
          extracted = (T) (Long) rawTimestamp.getTime();
          break;
        }
        case INCREMENTING_OFFSET: {
          TimestampIncrementingOffset offset = TimestampIncrementingOffset.fromMap(record.sourceOffset());
          extracted = (T) (Long) offset.getIncrementingOffset();
          break;
        }
        case TIMESTAMP_OFFSET: {
          TimestampIncrementingOffset offset = TimestampIncrementingOffset.fromMap(record.sourceOffset());
          Timestamp rawTimestamp = offset.getTimestampOffset();
          extracted = (T) (Long) rawTimestamp.getTime();
          break;
        }
        default:
          throw new RuntimeException("Invalid field");
      }
      Integer count = result.get(extracted);
      count = (count != null ? count : 0) + 1;
      result.put(extracted, count);
    }
    return result;
  }

  private Map<Integer, Integer> countIntValues(List<SourceRecord> records, String fieldName) {
    return countInts(records, Field.VALUE, fieldName);
  }

  private Map<Long, Integer> countTimestampValues(List<SourceRecord> records, String fieldName) {
    return countInts(records, Field.TIMESTAMP_VALUE, fieldName);
  }

  private Map<Long, Integer> countIntIncrementingOffsets(List<SourceRecord> records, String fieldName) {
    return countInts(records, Field.INCREMENTING_OFFSET, fieldName);
  }


  private void assertIncrementingOffsets(List<SourceRecord> records) {
    // Should use incrementing field as offsets
    for(SourceRecord record : records) {
      Object incrementing = ((Struct)record.value()).get("id");
      long incrementingValue = incrementing instanceof Integer ? (long)(Integer)incrementing
                                                           : (Long)incrementing;
      long offsetValue = TimestampIncrementingOffset.fromMap(record.sourceOffset()).getIncrementingOffset();
      assertEquals(incrementingValue, offsetValue);
    }
  }

  private void assertTimestampOffsets(List<SourceRecord> records) {
    // Should use timestamps as offsets
    for(SourceRecord record : records) {
      Timestamp timestampValue = (Timestamp) ((Struct)record.value()).get("modified");
      Timestamp offsetValue = TimestampIncrementingOffset.fromMap(record.sourceOffset()).getTimestampOffset();
      assertEquals(timestampValue, offsetValue);
    }
  }

  private void assertRecordsTopic(List<SourceRecord> records, String topic) {
    for (SourceRecord record : records) {
      assertEquals(topic, record.topic());
    }
  }

  private void assertRecordsSourcePartition(List<SourceRecord> records,
                                            Map<String, String> partition) {
    for (SourceRecord record : records) {
      assertEquals(partition, record.sourcePartition());
    }
  }

}
