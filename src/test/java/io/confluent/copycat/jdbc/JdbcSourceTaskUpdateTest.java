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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // This is currently the default mode
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

  Map<Integer, Integer> countIntValues(List<SourceRecord> records) {
    HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
    for (SourceRecord record : records) {
      Integer value = (Integer) ((GenericRecord) record.getValue()).get(0);
      Integer count = result.get(value);
      count = (count != null ? count : 0) + 1;
      result.put(value, count);
    }
    return result;
  }
}
