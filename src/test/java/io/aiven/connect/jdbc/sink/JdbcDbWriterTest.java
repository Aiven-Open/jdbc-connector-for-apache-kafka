/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.connect.jdbc.sink;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JdbcDbWriterTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private JdbcDbWriter writer = null;
    private DatabaseDialect dialect;

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        if (writer != null) {
            writer.closeQuietly();
        }
        sqliteHelper.tearDown();
    }

    private JdbcDbWriter newWriter(final Map<String, String> props) {
        final JdbcSinkConfig config = new JdbcSinkConfig(props);
        dialect = new SqliteDatabaseDialect(config);
        final DbStructure dbStructure = new DbStructure(dialect);
        return new JdbcDbWriter(config, dialect, dbStructure);
    }

    @Test
    public void shouldGenerateNormalizedTableNameForTopic() {
        final Map<String, Object> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhost");
        props.put(JdbcSinkConfig.TABLE_NAME_FORMAT, "kafka_topic_${topic}");
        props.put(JdbcSinkConfig.TABLE_NAME_NORMALIZE, true);
        final JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig(props);

        dialect = new SqliteDatabaseDialect(jdbcSinkConfig);
        final DbStructure dbStructure = new DbStructure(dialect);
        final JdbcDbWriter jdbcDbWriter = new JdbcDbWriter(jdbcSinkConfig, dialect, dbStructure);

        assertEquals("kafka_topic___some_topic",
                jdbcDbWriter.generateTableNameFor("--some_topic"));

        assertEquals("kafka_topic_some_topic",
                jdbcDbWriter.generateTableNameFor("some_topic"));

        assertEquals("kafka_topic_some_topic",
                jdbcDbWriter.generateTableNameFor("some-topic"));

        assertEquals("kafka_topic_this_is_topic_with_dots",
                jdbcDbWriter.generateTableNameFor("this.is.topic.with.dots"));

        assertEquals("kafka_topic_this_is_topic_with_dots_and_weired_characters___",
                jdbcDbWriter.generateTableNameFor("this.is.topic.with.dots.and.weired.characters#$%"));

        assertEquals("kafka_topic_orders_topic__3",
                jdbcDbWriter.generateTableNameFor("orders_topic_#3"));

    }

    @Test
    public void shouldSelectTableFromMapping() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhnost");
        props.put(JdbcSinkConfig.TABLE_NAME_FORMAT, "${topic}");
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "some_topic:same_table");

        final JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig(props);
        dialect = new SqliteDatabaseDialect(jdbcSinkConfig);
        final DbStructure dbStructure = new DbStructure(dialect);
        final JdbcDbWriter writer = new JdbcDbWriter(jdbcSinkConfig, dialect, dbStructure);

        final TableId tableId = writer.destinationTable("some_topic");
        assertEquals("same_table", tableId.tableName());
    }

    @Test(expected = ConnectException.class)
    public void shouldThrowConnectExceptionForUnknownTopicToTableMapping() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL_CONFIG, "jdbc://localhnost");
        props.put(JdbcSinkConfig.TABLE_NAME_FORMAT, "");
        props.put(JdbcSinkConfig.TOPICS_TO_TABLES_MAPPING, "some_topic:same_table,some_topic2:same_table2");

        final JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig(props);
        dialect = new SqliteDatabaseDialect(jdbcSinkConfig);
        final DbStructure dbStructure = new DbStructure(dialect);
        final JdbcDbWriter writer = new JdbcDbWriter(jdbcSinkConfig, dialect, dbStructure);
        writer.generateTableNameFor("another_topic");
    }

    @Test
    public void autoCreateWithAutoEvolve() throws SQLException {
        final String topic = "books";
        final TableId tableId = new TableId(null, null, topic);

        final Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("auto.evolve", "true");
        props.put("pk.mode", "record_key");
        props.put("pk.fields", "id"); // assigned name for the primitive key

        writer = newWriter(props);

        final Schema keySchema = Schema.INT64_SCHEMA;

        final Schema valueSchema1 = SchemaBuilder.struct()
            .field("author", Schema.STRING_SCHEMA)
            .field("title", Schema.STRING_SCHEMA)
            .build();

        final Struct valueStruct1 = new Struct(valueSchema1)
            .put("author", "Tom Robbins")
            .put("title", "Villa Incognito");

        writer.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema1,
            valueStruct1, 0)));

        final TableDefinition metadata = dialect.describeTable(writer.cachedConnectionProvider.getConnection(),
            tableId);
        assertTrue(metadata.definitionForColumn("id").isPrimaryKey());
        for (final Field field : valueSchema1.fields()) {
            assertNotNull(metadata.definitionForColumn(field.name()));
        }

        final Schema valueSchema2 = SchemaBuilder.struct()
            .field("author", Schema.STRING_SCHEMA)
            .field("title", Schema.STRING_SCHEMA)
            .field("year", Schema.OPTIONAL_INT32_SCHEMA) // new field
            .field("review", SchemaBuilder.string().defaultValue("").build()); // new field

        final Struct valueStruct2 = new Struct(valueSchema2)
            .put("author", "Tom Robbins")
            .put("title", "Fierce Invalids")
            .put("year", 2016);

        writer.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 2L, valueSchema2, valueStruct2, 0)));

        final TableDefinition refreshedMetadata = dialect.describeTable(sqliteHelper.connection, tableId);
        assertTrue(refreshedMetadata.definitionForColumn("id").isPrimaryKey());
        for (final Field field : valueSchema2.fields()) {
            assertNotNull(refreshedMetadata.definitionForColumn(field.name()));
        }
    }

    @Test(expected = SQLException.class)
    public void multiInsertWithKafkaPkFailsDueToUniqueConstraint() throws SQLException {
        writeSameRecordTwiceExpectingSingleUpdate(
            JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.KAFKA, "");
    }

    @Test
    public void idempotentUpsertWithKafkaPk() throws SQLException {
        writeSameRecordTwiceExpectingSingleUpdate(
            JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.KAFKA, "");
    }

    @Test(expected = SQLException.class)
    public void multiInsertWithRecordKeyPkFailsDueToUniqueConstraint() throws SQLException {
        writeSameRecordTwiceExpectingSingleUpdate(
            JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, "");
    }

    @Test
    public void idempotentUpsertWithRecordKeyPk() throws SQLException {
        writeSameRecordTwiceExpectingSingleUpdate(
            JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, "");
    }

    @Test(expected = SQLException.class)
    public void multiInsertWithRecordValuePkFailsDueToUniqueConstraint() throws SQLException {
        writeSameRecordTwiceExpectingSingleUpdate(
            JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE, "author,title");
    }

    @Test
    public void idempotentUpsertWithRecordValuePk() throws SQLException {
        writeSameRecordTwiceExpectingSingleUpdate(
            JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE, "author,title");
    }

    private void writeSameRecordTwiceExpectingSingleUpdate(
        final JdbcSinkConfig.InsertMode insertMode,
        final JdbcSinkConfig.PrimaryKeyMode pkMode,
        final String pkFields
    ) throws SQLException {
        final String topic = "books";
        final int partition = 7;
        final long offset = 42;

        final Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("pk.mode", pkMode.toString());
        props.put("pk.fields", pkFields);
        props.put("insert.mode", insertMode.toString());

        writer = newWriter(props);

        final Schema keySchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.INT64_SCHEMA);

        final Struct keyStruct = new Struct(keySchema).put("id", 0L);

        final Schema valueSchema = SchemaBuilder.struct()
            .field("author", Schema.STRING_SCHEMA)
            .field("title", Schema.STRING_SCHEMA)
            .build();

        final Struct valueStruct = new Struct(valueSchema)
            .put("author", "Tom Robbins")
            .put("title", "Villa Incognito");

        final SinkRecord record = new SinkRecord(
            topic, partition, keySchema, keyStruct, valueSchema, valueStruct, offset);

        writer.write(Collections.nCopies(2, record));

        assertEquals(
            1,
            sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
                @Override
                public void read(final ResultSet rs) throws SQLException {
                    assertEquals(1, rs.getInt(1));
                }
            })
        );
    }

    @Test
    public void sameRecordNTimes() throws SQLException {
        final String testId = "sameRecordNTimes";
        final String createTable = "CREATE TABLE " + testId + " ("
            + "    the_byte  INTEGER,"
            + "    the_short INTEGER,"
            + "    the_int INTEGER,"
            + "    the_long INTEGER,"
            + "    the_float REAL,"
            + "    the_double REAL,"
            + "    the_bool  INTEGER,"
            + "    the_string TEXT,"
            + "    the_bytes BLOB, "
            + "    the_decimal  NUMERIC,"
            + "    the_date  NUMERIC,"
            + "    the_time  NUMERIC,"
            + "    the_timestamp  NUMERIC"
            + ");";

        sqliteHelper.deleteTable(testId);
        sqliteHelper.createTable(createTable);

        final Schema schema = SchemaBuilder.struct().name(testId)
            .field("the_byte", Schema.INT8_SCHEMA)
            .field("the_short", Schema.INT16_SCHEMA)
            .field("the_int", Schema.INT32_SCHEMA)
            .field("the_long", Schema.INT64_SCHEMA)
            .field("the_float", Schema.FLOAT32_SCHEMA)
            .field("the_double", Schema.FLOAT64_SCHEMA)
            .field("the_bool", Schema.BOOLEAN_SCHEMA)
            .field("the_string", Schema.STRING_SCHEMA)
            .field("the_bytes", Schema.BYTES_SCHEMA)
            .field("the_decimal", Decimal.schema(2).schema())
            .field("the_date", Date.SCHEMA)
            .field("the_time", Time.SCHEMA)
            .field("the_timestamp", Timestamp.SCHEMA);

        final java.util.Date instant = new java.util.Date(1474661402123L);

        final Struct struct = new Struct(schema)
            .put("the_byte", (byte) -32)
            .put("the_short", (short) 1234)
            .put("the_int", 42)
            .put("the_long", 12425436L)
            .put("the_float", 2356.3f)
            .put("the_double", -2436546.56457d)
            .put("the_bool", true)
            .put("the_string", "foo")
            .put("the_bytes", new byte[]{-32, 124})
            .put("the_decimal", new BigDecimal("1234.567"))
            .put("the_date", instant)
            .put("the_time", instant)
            .put("the_timestamp", instant);

        final int numRecords = ThreadLocalRandom.current().nextInt(20, 80);

        final Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("table.name.format", testId);
        props.put("batch.size", String.valueOf(ThreadLocalRandom.current().nextInt(20, 100)));

        writer = newWriter(props);

        writer.write(Collections.nCopies(
            numRecords,
            new SinkRecord("topic", 0, null, null, schema, struct, 0)
        ));

        assertEquals(
            numRecords,
            sqliteHelper.select(
                "SELECT * FROM " + testId,
                new SqliteHelper.ResultSetReadCallback() {
                    @Override
                    public void read(final ResultSet rs) throws SQLException {
                        assertEquals(struct.getInt8("the_byte").byteValue(), rs.getByte("the_byte"));
                        assertEquals(struct.getInt16("the_short").shortValue(), rs.getShort("the_short"));
                        assertEquals(struct.getInt32("the_int").intValue(), rs.getInt("the_int"));
                        assertEquals(struct.getInt64("the_long").longValue(), rs.getLong("the_long"));
                        assertEquals(struct.getFloat32("the_float"), rs.getFloat("the_float"), 0.01);
                        assertEquals(struct.getFloat64("the_double"), rs.getDouble("the_double"), 0.01);
                        assertEquals(struct.getBoolean("the_bool"), rs.getBoolean("the_bool"));
                        assertEquals(struct.getString("the_string"), rs.getString("the_string"));
                        assertArrayEquals(struct.getBytes("the_bytes"), rs.getBytes("the_bytes"));
                        assertEquals(struct.get("the_decimal"), rs.getBigDecimal("the_decimal"));
                        assertEquals(
                            new java.sql.Date(((java.util.Date) struct.get("the_date")).getTime()),
                            rs.getDate("the_date"));
                        assertEquals(
                            new java.sql.Time(((java.util.Date) struct.get("the_time")).getTime()),
                            rs.getTime("the_time"));
                        assertEquals(
                            new java.sql.Timestamp(((java.util.Date) struct.get("the_time")).getTime()),
                            rs.getTimestamp("the_timestamp"));
                    }
                }
            )
        );
    }

}
