/*
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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;

import static org.junit.Assert.assertEquals;

public class BufferedRecordsTest {

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

  @Before
  public void setUp() throws IOException, SQLException {
    sqliteHelper.setUp();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    sqliteHelper.tearDown();
  }

  @Test
  public void correctBatching() throws SQLException {
    final DbDialect dbDialect = DbDialect.fromConnectionString(sqliteHelper.sqliteUri());
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final BufferedRecords buffer = new BufferedRecords(config, "dummy", dbDialect, dbStructure, sqliteHelper.connection);

    final Schema schemaA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    final Struct valueA = new Struct(schemaA)
        .put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);

    final Schema schemaB = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("name", "cuba")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, null, null, schemaB, valueB, 1);

    // test records are batched correctly based on schema equality as records are added
    //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));

    assertEquals(Arrays.asList(recordA, recordA, recordA), buffer.add(recordB));

    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }

}
