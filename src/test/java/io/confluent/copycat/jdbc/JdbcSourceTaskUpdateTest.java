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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.jdbc.EmbeddedDerby.ColumnName;
import io.confluent.copycat.jdbc.EmbeddedDerby.EqualsCondition;
import io.confluent.copycat.source.SourceRecord;

import static org.junit.Assert.assertEquals;

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
  public void testAutoincrement() throws Exception {
    expectInitializeNoOffsets(Arrays.asList((Object) SINGLE_TABLE_NAME));

    PowerMock.replayAll();

    String extraColumn = "col";
    // This test requires multiple columns so we can insert rows, but we just ignore the extra
    // column
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
                   extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);

    initializeTask();
    Properties taskConfig = singleTableConfig();
    taskConfig.setProperty(JdbcSourceConnectorConfig.AUTOINCREMENT_CONFIG, "true");
    task.start(taskConfig);

    List<SourceRecord> records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records));
    assertEquals(Collections.singletonMap(1, 1), countIntKeys(records));
    assertEquals(Collections.singletonMap(1, 1), countIntOffsets(records));

    // Adding records should result in only those records during the next poll()
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    HashMap<Integer, Integer> twoRecords = new HashMap<Integer, Integer>();
    twoRecords.put(2, 1);
    twoRecords.put(3, 1);
    records = task.poll();
    assertEquals(twoRecords, countIntValues(records));
    assertEquals(twoRecords, countIntKeys(records));
    assertEquals(twoRecords, countIntOffsets(records));

    PowerMock.verifyAll();
  }

  @Test
  public void testAutoincrementRestoreOffset() throws Exception {
    expectInitialize(Arrays.asList((Object) SINGLE_TABLE_NAME),
                     Collections.singletonMap((Object) SINGLE_TABLE_NAME, (Object) 1));

    PowerMock.replayAll();

    String extraColumn = "col";
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
                   extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    initializeTask();
    Properties taskConfig = singleTableConfig();
    taskConfig.setProperty(JdbcSourceConnectorConfig.AUTOINCREMENT_CONFIG, "true");
    task.start(taskConfig);

    HashMap<Integer, Integer> twoRecords = new HashMap<Integer, Integer>();
    twoRecords.put(2, 1);
    twoRecords.put(3, 1);
    List<SourceRecord> records = task.poll();
    assertEquals(twoRecords, countIntValues(records));
    assertEquals(twoRecords, countIntKeys(records));
    assertEquals(twoRecords, countIntOffsets(records));

    PowerMock.verifyAll();
  }

  private enum Field {
    KEY,
    VALUE,
    OFFSET
  }

  private Map<Integer, Integer> countInts(List<SourceRecord> records, Field field) {
    HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
    for (SourceRecord record : records) {
      Integer extracted;
      switch (field) {
        case KEY:
          extracted = (Integer)record.getKey();
          break;
        case VALUE:
          extracted = (Integer)((GenericRecord)record.getValue()).get(0);
          break;
        case OFFSET:
          extracted = (Integer)record.getOffset();
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

  private Map<Integer, Integer> countIntValues(List<SourceRecord> records) {
    return countInts(records, Field.VALUE);
  }

  private Map<Integer, Integer> countIntKeys(List<SourceRecord> records) {
    return countInts(records, Field.KEY);
  }

  private Map<Integer, Integer> countIntOffsets(List<SourceRecord> records) {
    return countInts(records, Field.OFFSET);
  }
}
