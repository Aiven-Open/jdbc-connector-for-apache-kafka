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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.sink.dialect.SqliteDialect;
import io.confluent.connect.jdbc.sink.metadata.DbTable;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class JdbcDbWriterTest {

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

  @Before
  public void setUp() throws IOException, SQLException {
    sqliteHelper.setUp();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    sqliteHelper.tearDown();
  }

  private JdbcDbWriter newWriter(Map<String, String> props) {
    final JdbcSinkConfig config = new JdbcSinkConfig(props);
    final DbDialect dbDialect = new SqliteDialect();
    final DbStructure dbStructure = new DbStructure(dbDialect);
    return new JdbcDbWriter(config, dbDialect, dbStructure);
  }

  @Test
  public void autoCreateWithAutoEvolve() throws SQLException {
    String topic = "books";

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "record_key");
    props.put("pk.fields", "id"); // assigned name for the primitive key

    JdbcDbWriter writer = newWriter(props);

    Schema keySchema = Schema.INT64_SCHEMA;

    Schema valueSchema1 = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build();

    Struct valueStruct1 = new Struct(valueSchema1)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    writer.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema1, valueStruct1, 0)));

    DbTable metadata = DbMetadataQueries.getTableMetadata(writer.connection, topic);
    assertTrue(metadata.columns.get("id").isPrimaryKey);
    for (Field field : valueSchema1.fields()) {
      assertTrue(metadata.columns.containsKey(field.name()));
    }

    Schema valueSchema2 = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .field("year", Schema.OPTIONAL_INT32_SCHEMA) // new field
        .field("review", SchemaBuilder.string().defaultValue("").build()); // new field

    Struct valueStruct2 = new Struct(valueSchema2)
        .put("author", "Tom Robbins")
        .put("title", "Fierce Invalids")
        .put("year", 2016);

    writer.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 2L, valueSchema2, valueStruct2, 0)));

    DbTable refreshedMetadata = DbMetadataQueries.getTableMetadata(sqliteHelper.connection, topic);
    assertTrue(metadata.columns.get("id").isPrimaryKey);
    for (Field field : valueSchema2.fields()) {
      assertTrue(refreshedMetadata.columns.containsKey(field.name()));
    }
  }

  @Test(expected = SQLException.class)
  public void multiInsertWithKafkaPkFailsDueToUniqueConstraint() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.KAFKA, "");
  }

  @Test
  public void idempotentUpsertWithKafkaPk() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.KAFKA, "");
  }

  @Test(expected = SQLException.class)
  public void multiInsertWithRecordKeyPkFailsDueToUniqueConstraint() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, "");
  }

  @Test
  public void idempotentUpsertWithRecordKeyPk() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, "");
  }

  @Test(expected = SQLException.class)
  public void multiInsertWithRecordValuePkFailsDueToUniqueConstraint() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE, "author,title");
  }

  @Test
  public void idempotentUpsertWithRecordValuePk() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE, "author,title");
  }

  private void writeSameRecordTwiceExpectingSingleUpdate(
      JdbcSinkConfig.InsertMode insertMode,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      String pkFields
  ) throws SQLException {
    String topic = "books";
    int partition = 7;
    long offset = 42;

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("pk.mode", pkMode.toString());
    props.put("pk.fields", pkFields);
    props.put("insert.mode", insertMode.toString());

    JdbcDbWriter writer = newWriter(props);

    Schema keySchema = SchemaBuilder.struct()
        .field("id", SchemaBuilder.INT64_SCHEMA);

    Struct keyStruct = new Struct(keySchema).put("id", 0L);

    Schema valueSchema = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build();

    Struct valueStruct = new Struct(valueSchema)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    SinkRecord record = new SinkRecord(topic, partition, keySchema, keyStruct, valueSchema, valueStruct, offset);

    writer.write(Collections.nCopies(2, record));

    assertEquals(
        1,
        sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
          @Override
          public void read(ResultSet rs) throws SQLException {
            assertEquals(1, rs.getInt(1));
          }
        })
    );
  }

  @Test
  public void sameRecordNTimes() throws SQLException {
    String tableName = "batched_statement_test_100";
    String createTable = "CREATE TABLE " + tableName + " (" +
                         "    firstName  TEXT," +
                         "    lastName  TEXT," +
                         "    age INTEGER," +
                         "    bool  NUMERIC," +
                         "    byte  INTEGER," +
                         "    short INTEGER," +
                         "    long INTEGER," +
                         "    float NUMERIC," +
                         "    double NUMERIC," +
                         "    bytes BLOB " +
                         ");";

    sqliteHelper.deleteTable(tableName);
    sqliteHelper.createTable(createTable);

    Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("short", Schema.OPTIONAL_INT16_SCHEMA)
        .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
        .field("long", Schema.OPTIONAL_INT64_SCHEMA)
        .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("bytes", Schema.OPTIONAL_BYTES_SCHEMA);

    final Struct struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("bool", true)
        .put("short", (short) 1234)
        .put("byte", (byte) -32)
        .put("long", 12425436L)
        .put("float", 2356.3f)
        .put("double", -2436546.56457d)
        .put("bytes", new byte[]{-32, 124});

    int numRecords = ThreadLocalRandom.current().nextInt(20, 80);

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("table.name.format", tableName);
    props.put("batch.size", String.valueOf(ThreadLocalRandom.current().nextInt(20, 100)));

    JdbcDbWriter writer = newWriter(props);

    writer.write(Collections.nCopies(
        numRecords,
        new SinkRecord("topic", 0, null, null, schema, struct, 0)
    ));

    assertEquals(
        numRecords,
        sqliteHelper.select(
            "SELECT * FROM " + tableName + " ORDER BY firstName",
            new SqliteHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                assertEquals(struct.getString("firstName"), rs.getString("firstName"));
                assertEquals(struct.getString("lastName"), rs.getString("lastName"));
                assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                rs.getInt("age");
                assertTrue(rs.wasNull());
                assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
                assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                assertTrue(Arrays.equals(struct.getBytes("bytes"), rs.getBytes("bytes")));
              }
            }
        )
    );
  }

}