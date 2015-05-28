/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.copycat.jdbc;

import org.junit.After;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.data.GenericRecordBuilder;
import io.confluent.copycat.jdbc.EmbeddedDerby.ColumnName;
import io.confluent.copycat.jdbc.EmbeddedDerby.EqualsCondition;
import io.confluent.copycat.source.SourceRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// Tests of polling that return data updates, i.e. verifies the different behaviors for getting
// incremental data updates from the database
public class JdbcSourceTaskUpdateTest extends JdbcSourceTaskTestBase {

  @After
  public void tearDown() throws Exception {
    task.stop();
    super.tearDown();
  }

  @Test
  public void testBulkPeriodicLoad() throws Exception {
    ColumnName column = new ColumnName("id");
    db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);

    // Bulk periodic load is currently the default
    task.start(singleTableConfig());

    List<SourceRecord> records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records));

    records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records));

    db.insert(SINGLE_TABLE_NAME, "id", 2);
    records = task.poll();
    HashMap<Integer, Integer> twoRecords = new HashMap<Integer, Integer>();
    twoRecords.put(1, 1);
    twoRecords.put(2, 1);
    assertEquals(twoRecords, countIntValues(records));

    db.delete(SINGLE_TABLE_NAME, new EqualsCondition(column, 1));
    records = task.poll();
    assertEquals(Collections.singletonMap(2, 1), countIntValues(records));
  }

  @Test
  public void testManualIncreasing() throws Exception {
    expectInitializeNoOffsets(Arrays.asList((Object) SINGLE_TABLE_NAME));

    PowerMock.replayAll();

    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);

    startTask(null, "id");
    verifyIncreasingFirstPoll();

    // Adding records should result in only those records during the next poll()
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 3);

    verifyPoll(2, "id", Arrays.asList(2, 3), false, true);

    PowerMock.verifyAll();
  }

  @Test
  public void testAutoincrement() throws Exception {
    expectInitializeNoOffsets(Arrays.asList((Object) SINGLE_TABLE_NAME));

    PowerMock.replayAll();

    String extraColumn = "col";
    // Need extra column to be able to insert anything, extra is ignored.
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
                   extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);

    startTask(null, ""); // auto-incrementing
    verifyIncreasingFirstPoll();

    // Adding records should result in only those records during the next poll()
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    verifyPoll(2, "id", Arrays.asList(2, 3), false, true);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestamp() throws Exception {
    expectInitializeNoOffsets(Arrays.asList((Object) SINGLE_TABLE_NAME));

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT");
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(10L).toString(), "id", 1);

    startTask("modified", null);
    verifyTimestampFirstPoll();

    // If there isn't enough resolution, this could miss some rows. In this case, we'll only see
    // IDs 3 & 4.
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(10L).toString(), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(11L).toString(), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(12L).toString(), "id", 4);

    verifyPoll(2, "id", Arrays.asList(3, 4), true, false);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampAndIncreasing() throws Exception {
    expectInitializeNoOffsets(Arrays.asList((Object) SINGLE_TABLE_NAME));

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(10L).toString(), "id", 1);

    startTask("modified", "id");
    verifyIncreasingAndTimestampFirstPoll();

    // Should be able to pick up id 2 because of ID despite same timestamp.
    // Note this is setup so we can reuse some validation code
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(10L).toString(), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(11L).toString(), "id", 1);

    verifyPoll(2, "id", Arrays.asList(3, 1), true, true);

    PowerMock.verifyAll();
  }

  @Test
  public void testManualIncreasingRestoreOffset() throws Exception {
    GenericRecord offset = new GenericRecordBuilder(JdbcSourceTask.offsetSchema)
        .set(JdbcSourceTask.INCREASING_FIELD, 1L).build();
    expectInitialize(Arrays.asList((Object) SINGLE_TABLE_NAME),
                     Collections.singletonMap((Object) SINGLE_TABLE_NAME, (Object) offset));

    PowerMock.replayAll();

    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 3);

    startTask(null, "id");

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(2, 3), false, true);

    PowerMock.verifyAll();
  }

  @Test
  public void testAutoincrementRestoreOffset() throws Exception {
    GenericRecord offset = new GenericRecordBuilder(JdbcSourceTask.offsetSchema)
        .set(JdbcSourceTask.INCREASING_FIELD, 1L).build();
    expectInitialize(Arrays.asList((Object) SINGLE_TABLE_NAME),
                     Collections.singletonMap((Object) SINGLE_TABLE_NAME, (Object) offset));

    PowerMock.replayAll();

    String extraColumn = "col";
    // Use BIGINT here to test LONG columns
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY",
                   extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    startTask(null, ""); // autoincrementing

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(2L, 3L), false, true);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampRestoreOffset() throws Exception {
    GenericRecord offset = new GenericRecordBuilder(JdbcSourceTask.offsetSchema)
        .set(JdbcSourceTask.TIMESTAMP_FIELD, 10L).build();
    expectInitialize(Arrays.asList((Object) SINGLE_TABLE_NAME),
                     Collections.singletonMap((Object) SINGLE_TABLE_NAME, (Object) offset));

    PowerMock.replayAll();

    // Timestamp is managed manually here so we can verify handling of duplicate values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT");
    // id=2 will be ignored since it has the same timestamp as the initial offset.
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(10L).toString(), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(11L).toString(), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(12L).toString(), "id", 4);

    startTask("modified", null);

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(3, 4), true, false);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampAndIncreasingRestoreOffset() throws Exception {
    GenericRecord offset = new GenericRecordBuilder(JdbcSourceTask.offsetSchema)
        .set(JdbcSourceTask.TIMESTAMP_FIELD, 10L)
        .set(JdbcSourceTask.INCREASING_FIELD, 3L)
        .build();
    expectInitialize(Arrays.asList((Object) SINGLE_TABLE_NAME),
                     Collections.singletonMap((Object) SINGLE_TABLE_NAME, (Object) offset));

    PowerMock.replayAll();

    // Timestamp is managed manually here so we can verify handling of duplicate values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT NOT NULL");
    // id=3 will be ignored since it has the same timestamp + id as the initial offset, rest
    // should be included, including id=1 which is an old ID with newer timestamp
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(9L).toString(), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(10L).toString(), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(11L).toString(), "id", 4);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(12L).toString(), "id", 5);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(13L).toString(), "id", 1);

    startTask("modified", "id");

    verifyPoll(3, "id", Arrays.asList(4, 5, 1), true, true);

    PowerMock.verifyAll();
  }



  private void startTask(String timestampColumn, String increasingColumn) {
    String mode = null;
    if (timestampColumn != null && increasingColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREASING;
    } else if (timestampColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP;
    } else if (increasingColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_INCREASING;
    } else {
      fail("Invalid task config");
    }
    initializeTask();
    Properties taskConfig = singleTableConfig();
    taskConfig.setProperty(JdbcSourceConnectorConfig.MODE_CONFIG, mode);
    if (timestampColumn != null) {
      taskConfig.setProperty(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG,
                             timestampColumn);
    }
    if (increasingColumn != null) {
      taskConfig.setProperty(JdbcSourceConnectorConfig.INCREASING_COLUMN_NAME_CONFIG,
                             increasingColumn);
    }
    task.start(taskConfig);
  }

  private void verifyIncreasingFirstPoll() throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records));
    assertEquals(Collections.singletonMap(1L, 1), countIntIncreasingOffsets(records));
    assertIncreasingOffsets(records);
  }

  private List<SourceRecord> verifyTimestampFirstPoll() throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertEquals(Collections.singletonMap(10L, 1), countLongValues(records, "modified"));
    assertTimestampOffsets(records);
    return records;
  }

  private void verifyIncreasingAndTimestampFirstPoll() throws Exception {
    List<SourceRecord> records = verifyTimestampFirstPoll();
    assertIncreasingOffsets(records);
  }

  private <T> void verifyPoll(int numRecords, String valueField, List<T> values,
                          boolean timestampOffsets, boolean increasingOffsets)
      throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(numRecords, records.size());

    HashMap<T, Integer> valueCounts = new HashMap<T, Integer>();
    for(T value : values) {
      valueCounts.put(value, 1);
    }
    assertEquals(valueCounts, countIntValues(records, valueField));

    if (timestampOffsets) {
      assertTimestampOffsets(records);
    }
    if (increasingOffsets) {
      assertIncreasingOffsets(records);
    }
  }

  private enum Field {
    KEY,
    VALUE,
    INCREASING_OFFSET,
    TIMESTAMP_OFFSET
  }

  private <T> Map<T, Integer> countInts(List<SourceRecord> records, Field field, String fieldName) {
    HashMap<T, Integer> result = new HashMap<T, Integer>();
    for (SourceRecord record : records) {
      T extracted;
      switch (field) {
        case KEY:
          extracted = (T)record.getKey();
          break;
        case VALUE:
          extracted = fieldName != null ? (T)((GenericRecord)record.getValue()).get(fieldName) :
                      (T)((GenericRecord)record.getValue()).get(0);
          break;
        case INCREASING_OFFSET:
          extracted = (T)((GenericRecord)record.getOffset()).get(JdbcSourceTask.INCREASING_FIELD);
          break;
        case TIMESTAMP_OFFSET:
          extracted = (T)((GenericRecord)record.getOffset()).get(JdbcSourceTask.TIMESTAMP_FIELD);
          break;
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

  private Map<Integer, Integer> countIntValues(List<SourceRecord> records) {
    return countInts(records, Field.VALUE, null);
  }

  private Map<Long, Integer> countLongValues(List<SourceRecord> records, String fieldName) {
    return countInts(records, Field.VALUE, fieldName);
  }

  private Map<Integer, Integer> countIntIncreasingOffsets(List<SourceRecord> records) {
    return countInts(records, Field.INCREASING_OFFSET, null);
  }


  private void assertIncreasingOffsets(List<SourceRecord> records) {
    // Should use increasing field as offsets
    for(SourceRecord record : records) {
      Object increasing = ((GenericRecord)record.getValue()).get("id");
      long increasingValue = increasing instanceof Integer ? (long)(Integer)increasing
                                                           : (Long)increasing;
      long offsetValue = (Long)((GenericRecord) record.getOffset())
          .get(JdbcSourceTask.INCREASING_FIELD);
      assertEquals(increasingValue, offsetValue);
    }
  }

  private void assertTimestampOffsets(List<SourceRecord> records) {
    // Should use timestamps as offsets
    for(SourceRecord record : records) {
      long timestampValue = (Long)((GenericRecord)record.getValue()).get("modified");
      long offsetValue = (Long)((GenericRecord) record.getOffset())
          .get(JdbcSourceTask.TIMESTAMP_FIELD);
      assertEquals(timestampValue, offsetValue);
    }
  }

}
