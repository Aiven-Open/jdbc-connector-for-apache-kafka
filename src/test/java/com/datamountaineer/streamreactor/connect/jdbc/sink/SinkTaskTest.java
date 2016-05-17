package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.*;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import com.google.common.collect.*;
import org.apache.kafka.common.*;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.*;
import org.junit.*;
import org.mockito.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DRIVER_MANAGER_CLASS;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.EXPORT_MAPPINGS;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.JAR_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Created by andrew@datamountaineer.com on 15/05/16.
 * kafka-connect-jdbc
 */
public class SinkTaskTest {
  private static final String DB_FILE = "test_db_writer_sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;

  static {
    try {
      JdbcDriverLoader.load("org.sqlite.JDBC", Paths.get(JdbcDbWriterTest.class.getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toFile());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() {
    deleteSqlLiteFile();
  }

  @After
  public void tearDown() {
    deleteSqlLiteFile();
  }

  private void deleteSqlLiteFile() {
    new File(DB_FILE).delete();
  }


  @Test
  public void TestSinkTaskStarts() throws SQLException {
    String tableName1 = "batched_upsert_test_1";
    String tableName2 = "batched_upsert_test_2";
    String topic1 = "topic1";
    String topic2 = "topic2";
    TopicPartition tp1 = new TopicPartition(topic1, 12);
    TopicPartition tp2 = new TopicPartition(topic2, 13);
    HashSet<TopicPartition> assignment = Sets.newHashSet();

    //Set topic assignments, used by the sinkContext mock
    assignment.add(tp1);
    assignment.add(tp2);

    //mock the context
    SinkTaskContext context = Mockito.mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(assignment);

    String selected = String.format("{%s:%s;*},{%s:%s;*}", topic1, tableName1, topic2, tableName2);
    String driver = null;
    try {
      driver = Paths.get(getClass().getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toAbsolutePath().toString();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    Map<String, String> props = new HashMap<>();
    props.put(DATABASE_CONNECTION_URI, SQL_LITE_URI);
    props.put(JAR_FILE, driver);
    props.put(DRIVER_MANAGER_CLASS, "org.sqlite.JDBC");
    props.put(EXPORT_MAPPINGS, selected);

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(context);

    String createTable1 = "CREATE TABLE " + tableName1 + "(" +
        "    firstName  TEXT PRIMARY_KEY," +
        "    lastName  TEXT PRIMARY_KEY," +
        "    age INTEGER," +
        "    bool  NUMERIC," +
        "    byte  INTEGER," +
        "    short INTEGER," +
        "    long INTEGER," +
        "    float NUMERIC," +
        "    double NUMERIC," +
        "    bytes BLOB);";

    String createTable2 = "CREATE TABLE " + tableName2 + "(" +
        "    firstName  TEXT PRIMARY_KEY," +
        "    lastName  TEXT PRIMARY_KEY," +
        "    age INTEGER," +
        "    bool  NUMERIC," +
        "    byte  INTEGER," +
        "    short INTEGER," +
        "    long INTEGER," +
        "    float NUMERIC," +
        "    double NUMERIC," +
        "    bytes BLOB);";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName1);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable1);
    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName2);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable2);

    task.start(props);

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

    final String fName1 = "Alex";
    final String lName1 = "Smith";
    final int age1 = 21;
    final boolean bool1 = true;
    final short s1 = 1234;
    final byte b1 = -32;
    final long l1 = 12425436;
    final float f1 = (float) 2356.3;
    final double d1 = -2436546.56457;
    final byte[] bs1 = new byte[]{-32, 124};

    Struct struct1 = new Struct(schema)
        .put("firstName", fName1)
        .put("lastName", lName1)
        .put("bool", bool1)
        .put("short", s1)
        .put("byte", b1)
        .put("long", l1)
        .put("float", f1)
        .put("double", d1)
        .put("bytes", bs1)
        .put("age", age1);
    final short s1a = s1 + 1;
    Struct struct1a = struct1.put("short", s1a)
        .put("float", f1 + 1)
        .put("double", d1 + 1);

    final String fName2 = "Christina";
    final String lName2 = "Brams";
    final int age2 = 28;
    final boolean bool2 = false;
    final byte b2 = -72;
    final long l2 = 8594;
    final double d2 = 3256677.56457;

    Struct struct2 = new Struct(schema)
        .put("firstName", fName2)
        .put("lastName", lName2)
        .put("bool", bool2)
        .put("byte", b2)
        .put("long", l2)
        .put("double", d2)
        .put("age", age2);


    int partition = 2;
    Collection<SinkRecord> records = Lists.newArrayList(
        new SinkRecord(topic1, partition, null, null, schema, struct1, 1),
        new SinkRecord(topic2, partition, null, null, schema, struct2, 2),
        new SinkRecord(topic1, partition, null, null, schema, struct1a, 3),
        new SinkRecord(topic2, partition, null, null, schema, struct2, 4));

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic1.toLowerCase(),
        new StructFieldsDataExtractor(new FieldsMappings(tableName1, topic1, true, new HashMap<String, FieldAlias>())));
    map.put(topic2.toLowerCase(),
        new StructFieldsDataExtractor(new FieldsMappings(tableName2, topic2, true, new HashMap<String, FieldAlias>())));

    task.put(records);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    String query = "SELECT * FROM " + tableName1 + " WHERE firstName='" + fName1 + "' and lastName='" + lName1 + "'";

    SqlLiteHelper.select(SQL_LITE_URI, query, new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {

        assertEquals(rs.getBoolean("bool"), bool1);
        assertEquals(rs.getShort("short"), s1a);
        assertEquals(rs.getByte("byte"), b1);
        assertEquals(rs.getLong("long"), l1);
        assertTrue(Float.compare(rs.getFloat("float"), f1 + 1) == 0);
        assertEquals(Double.compare(rs.getDouble("double"), d1 + 1), 0);
        assertEquals(rs.getInt("age"), age1);
      }
    });
  }
}
