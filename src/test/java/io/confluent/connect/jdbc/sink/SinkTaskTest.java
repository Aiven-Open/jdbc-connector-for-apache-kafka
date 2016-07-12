package io.confluent.connect.jdbc.sink;

import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import io.confluent.connect.jdbc.sink.config.FieldsMappings;
import io.confluent.connect.jdbc.sink.config.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.services.EmbeddedSingleNodeKafkaCluster;
import io.confluent.connect.jdbc.sink.services.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.RestService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class SinkTaskTest {
  private static final String DB_FILE = "test_db_writer_sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;

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
  public void TestSinkTaskStarts() throws Exception {
    TestBase base = new TestBase();
    TopicPartition tp1 = new TopicPartition(base.getTopic1(), 12);
    TopicPartition tp2 = new TopicPartition(base.getTopic2(), 13);
    HashSet<TopicPartition> assignment = new HashSet<>();

    //Set topic assignments, used by the sinkContext mock
    assignment.add(tp1);
    assignment.add(tp2);

    //mock the context
    SinkTaskContext context = Mockito.mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(assignment);

    int port = InstanceSpec.getRandomPort();
    EmbeddedSingleNodeKafkaCluster cluster = new EmbeddedSingleNodeKafkaCluster();
    RestApp registry = new RestApp(port, cluster.zookeeperConnect(), base.getTopic1());
    registry.start();
    RestService client = registry.restClient;

    String rawSchema = "{\"type\":\"record\",\"name\":\"myrecord\",\n" +
                       "\"fields\":[\n" +
                       "{\"name\":\"firstName\",\"type\":[\"null\", \"string\"]},\n" +
                       "{\"name\":\"lastName\", \"type\": \"string\"}, \n" +
                       "{\"name\":\"age\", \"type\": \"int\"}, \n" +
                       "{\"name\":\"bool\", \"type\": \"float\"},\n" +
                       "{\"name\":\"byte\", \"type\": \"float\"},\n" +
                       "{\"name\":\"short\", \"type\": [\"null\", \"int\"]},\n" +
                       "{\"name\":\"long\", \"type\": \"long\"},\n" +
                       "{\"name\":\"float\", \"type\": \"float\"},\n" +
                       "{\"name\":\"double\", \"type\": \"double\"}\n" +
                       "]}";

    //register the schema for topic1
    client.registerSchema(rawSchema, base.getTopic1());
    Map<String, String> props = base.getPropsAllFields("insert", false);
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_URI, SQL_LITE_URI);
    props.put(JdbcSinkConfig.EXPORT_MAPPINGS,
              "INSERT INTO " + base.getTableName1() + " SELECT * FROM " + base.getTopic1() + " AUTOCREATE;" +
              "INSERT INTO " + base.getTableName2() + " SELECT * FROM " + base.getTopic2());

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(context);

    String createTable2 = "CREATE TABLE " + base.getTableName2() + "(" +
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

    SqlLiteHelper.deleteTable(SQL_LITE_URI, base.getTableName1());
    SqlLiteHelper.deleteTable(SQL_LITE_URI, base.getTableName2());
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
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA);

    final String fName1 = "Alex";
    final String lName1 = "Smith";
    final int age1 = 21;
    final boolean bool1 = true;
    final short s1 = 1234;
    final byte b1 = -32;
    final long l1 = 12425436;
    final float f1 = (float) 2356.3;
    final double d1 = -2436546.56457;

    Struct struct1 = new Struct(schema)
        .put("firstName", fName1)
        .put("lastName", lName1)
        .put("bool", bool1)
        .put("short", s1)
        .put("byte", b1)
        .put("long", l1)
        .put("float", f1)
        .put("double", d1)
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

    int partition = 1;
    Collection<SinkRecord> records = Arrays.asList(
        new SinkRecord(base.getTopic2(), partition, null, null, schema, struct2, 2),
        new SinkRecord(base.getTopic1(), partition, null, null, schema, struct1a, 3));
    task.put(records);

    Thread.sleep(300); //SQLite delay
    String query = "SELECT * FROM " + base.getTableName2() + " WHERE firstName='" + fName1 + "' and lastName='" + lName1 + "'";

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

    final String topic1 = base.getTopic1();
    SqlLiteHelper.select(SQL_LITE_URI, "SELECT " + FieldsMappings.CONNECT_TOPIC_COLUMN + "," +
                                       FieldsMappings.CONNECT_OFFSET_COLUMN + "," +
                                       FieldsMappings.CONNECT_PARTITION_COLUMN + " FROM " + base.getTableName1(), new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {
        assertEquals(rs.getInt(FieldsMappings.CONNECT_PARTITION_COLUMN), 1);
        assertEquals(rs.getLong(FieldsMappings.CONNECT_OFFSET_COLUMN), 3);
        assertEquals(rs.getString(FieldsMappings.CONNECT_TOPIC_COLUMN), topic1);
      }
    });
  }
}
