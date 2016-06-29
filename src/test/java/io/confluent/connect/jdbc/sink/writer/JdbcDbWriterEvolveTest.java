package io.confluent.connect.jdbc.sink.writer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.ConnectionProvider;
import io.confluent.connect.jdbc.sink.Database;
import io.confluent.connect.jdbc.sink.RecordDataExtractor;
import io.confluent.connect.jdbc.sink.SqlLiteHelper;
import io.confluent.connect.jdbc.sink.common.DatabaseMetadata;
import io.confluent.connect.jdbc.sink.common.DbTable;
import io.confluent.connect.jdbc.sink.common.DbTableColumn;
import io.confluent.connect.jdbc.sink.config.FieldAlias;
import io.confluent.connect.jdbc.sink.config.FieldsMappings;
import io.confluent.connect.jdbc.sink.config.InsertModeEnum;
import io.confluent.connect.jdbc.sink.dialect.SQLiteDialect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JdbcDbWriterEvolveTest {
  private static final String DB_FILE = "sqllite-jdbc-writer-evolve-test.db";
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
  public void handleTableWhereAColumnIsAddedAndThenSchemaChangesToContainTheNewFieldAutomaticallyMappingToTheColumn() throws SQLException {
    String tableName = "column_added_scenario";
    String createTable = "CREATE TABLE " + tableName + " (" +
                         "    firstName  TEXT," +
                         "    lastName  TEXT," +
                         "    age INTEGER," +
                         "    PRIMARY KEY(firstName, lastName)" +
                         ");";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable);

    Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA);

    final String fName1 = "Alex";
    final String lName1 = "Smith";
    final int age1 = 21;

    Struct struct1 = new Struct(schema)
        .put("firstName", fName1)
        .put("lastName", lName1)
        .put("age", age1);

    final String topic = "topic";
    final int partition = 2;

    QueryBuilder queryBuilder = new InsertQueryBuilder(new SQLiteDialect());
    Map<String, DataExtractorWithQueryBuilder> map = new HashMap<>();
    Map<String, FieldAlias> fields = new HashMap<>();
    map.put(topic.toLowerCase(),
            new DataExtractorWithQueryBuilder(
                queryBuilder,
                new RecordDataExtractor(new FieldsMappings(tableName, topic, true, InsertModeEnum.INSERT, fields, false, true, false))));

    List<DbTable> dbTables = Collections.emptyList();
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    ConnectionProvider connectionProvider = new ConnectionProvider(SQL_LITE_URI, null, null, 5, 100);
    Database executor = new Database(
        Collections.singleton(tableName),
        Collections.<String>emptySet(),
        dbMetadata,
        new SQLiteDialect(),
        1);
    JdbcDbWriter writer = new JdbcDbWriter(
        connectionProvider,
        new PreparedStatementContextIterable(map, 1000),
        new ThrowErrorHandlingPolicy(),
        executor,
        10);

    writer.write(Collections.singletonList(
        new SinkRecord(topic, partition, null, null, schema, struct1, 0)
    ));

    String query = "SELECT * FROM " + tableName + " ORDER BY firstName";
    SqlLiteHelper.ResultSetReadCallback callback = new SqlLiteHelper.ResultSetReadCallback() {

      @Override
      public void read(ResultSet rs) throws SQLException {
        assertEquals(rs.getString("firstName"), fName1);
        assertEquals(rs.getString("lastName"), lName1);
        assertEquals(rs.getInt("age"), age1);
      }
    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);

    SqlLiteHelper.execute(SQL_LITE_URI, "ALTER TABLE " + tableName + " ADD COLUMN salary REAL NULL;");

    Schema schema2 = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .field("salary", Schema.OPTIONAL_FLOAT64_SCHEMA);

    final String fName2 = "Jane";
    final String lName2 = "Wood";
    final int age2 = 28;
    final double salary2 = 1956.15;
    Struct struct2 = new Struct(schema2)
        .put("firstName", fName2)
        .put("lastName", lName2)
        .put("age", age2)
        .put("salary", salary2);

    writer.write(Collections.singletonList(
        new SinkRecord(topic, partition, null, null, schema2, struct2, 0)
    ));

    query = "SELECT * FROM " + tableName + " WHERE salary = null";
    callback = new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {
        assertEquals(rs.getString("firstName"), fName2);
        assertEquals(rs.getString("lastName"), lName2);
        assertEquals(rs.getInt("age"), age2);
        assertTrue(Double.compare(rs.getDouble("salary"), salary2) == 0);
      }
    };
    SqlLiteHelper.select(SQL_LITE_URI, query, callback);
  }

  @Test
  public void handleTabeEvolutionWhenRecordsWithDifferentSchemaEvolutionArePassed() throws SQLException {
    String tableName = "column_added_multipe_records";
    String createTable = "CREATE TABLE " + tableName + " (" +
                         "    firstName  TEXT," +
                         "    lastName  TEXT," +
                         "    age INTEGER," +
                         "    PRIMARY KEY(firstName, lastName)" +
                         ");";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable);

    Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA);

    final String fName1 = "Alex";
    final String lName1 = "Smith";
    final int age1 = 21;

    Struct struct1 = new Struct(schema)
        .put("firstName", fName1)
        .put("lastName", lName1)
        .put("age", age1);

    final String topic = "topic";
    final int partition = 2;

    QueryBuilder queryBuilder = new InsertQueryBuilder(new SQLiteDialect());
    Map<String, DataExtractorWithQueryBuilder> map = new HashMap<>();
    Map<String, FieldAlias> fields = new HashMap<>();
    map.put(topic.toLowerCase(),
            new DataExtractorWithQueryBuilder(
                queryBuilder,
                new RecordDataExtractor(new FieldsMappings(tableName, topic, true, InsertModeEnum.INSERT, fields, false, true, false))));

    List<DbTable> dbTables = Collections.singletonList(
        new DbTable(tableName, Arrays.asList(
            new DbTableColumn("firstName", true, false, 0),
            new DbTableColumn("lastName", true, false, 0),
            new DbTableColumn("age", false, false, 0)
        ))
    );
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    ConnectionProvider connectionProvider = new ConnectionProvider(SQL_LITE_URI, null, null, 5, 100);
    Database executor = new Database(
        Collections.<String>emptySet(),
        Collections.singleton(tableName),
        dbMetadata,
        new SQLiteDialect(),
        1);
    JdbcDbWriter writer = new JdbcDbWriter(
        connectionProvider,
        new PreparedStatementContextIterable(map, 1000),
        new ThrowErrorHandlingPolicy(),
        executor,
        10);

    Schema schema2 = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .field("salary", Schema.OPTIONAL_FLOAT64_SCHEMA);

    final String fName2 = "Jane";
    final String lName2 = "Wood";
    final int age2 = 28;
    final double salary2 = 1956.15;
    Struct struct2 = new Struct(schema2)
        .put("firstName", fName2)
        .put("lastName", lName2)
        .put("age", age2)
        .put("salary", salary2);

    writer.write(Arrays.asList(
        new SinkRecord(topic, partition, null, null, schema, struct1, 0),
        new SinkRecord(topic, partition, null, null, schema2, struct2, 1)
    ));

    String query = "SELECT * FROM " + tableName + " ORDER BY firstName ASC";

    SqlLiteHelper.ResultSetReadCallback callback = new SqlLiteHelper.ResultSetReadCallback() {
      int index = 0;

      @Override
      public void read(ResultSet rs) throws SQLException {
        if (index == 0) {
          assertEquals(rs.getString("firstName"), fName1);
          assertEquals(rs.getString("lastName"), lName1);
          assertEquals(rs.getInt("age"), age1);
          assertNull(rs.getObject("salary"));
        } else {
          assertEquals(rs.getString("firstName"), fName2);
          assertEquals(rs.getString("lastName"), lName2);
          assertEquals(rs.getInt("age"), age2);
          assertTrue(Double.compare(rs.getDouble("salary"), salary2) == 0);
        }
        index++;
      }
    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);
  }

  @Test
  public void handleBatchStatementPerRecordInsertingWithAutoCreatedColumnForPK() throws SQLException {
    String tableName = "batch_100_auto_create_column";

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

    final String topic = "topic";
    final int partition = 2;
    List<SinkRecord> records = new ArrayList<>(100);
    for (int i = 0; i < 100; ++i) {
      records.add(new SinkRecord(topic, partition, null, null, schema, struct1, i));
    }

    QueryBuilder queryBuilder = new InsertQueryBuilder(new SQLiteDialect());
    Map<String, DataExtractorWithQueryBuilder> map = new HashMap<>();
    Map<String, FieldAlias> fields = new HashMap<>();
    fields.put(FieldsMappings.CONNECT_TOPIC_COLUMN, new FieldAlias(FieldsMappings.CONNECT_TOPIC_COLUMN, true));
    fields.put(FieldsMappings.CONNECT_PARTITION_COLUMN, new FieldAlias(FieldsMappings.CONNECT_PARTITION_COLUMN, true));
    fields.put(FieldsMappings.CONNECT_OFFSET_COLUMN, new FieldAlias(FieldsMappings.CONNECT_OFFSET_COLUMN, true));
    map.put(topic.toLowerCase(),
            new DataExtractorWithQueryBuilder(
                queryBuilder,
                new RecordDataExtractor(new FieldsMappings(tableName, topic, true,
                                                           InsertModeEnum.INSERT,
                                                           fields,
                                                           true, true, false))));

    List<DbTable> dbTables = Collections.emptyList();
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    ConnectionProvider connectionProvider = new ConnectionProvider(SQL_LITE_URI, null, null, 5, 100);
    Database executor = new Database(
        Collections.singleton(tableName),
        Collections.<String>emptySet(),
        dbMetadata,
        new SQLiteDialect(),
        1);
    JdbcDbWriter writer = new JdbcDbWriter(
        connectionProvider,
        new PreparedStatementContextIterable(map, 1000),
        new ThrowErrorHandlingPolicy(),
        executor,
        10);

    writer.write(records);

    String query = "SELECT * FROM " + tableName + " ORDER BY firstName";
    SqlLiteHelper.ResultSetReadCallback callback = new SqlLiteHelper.ResultSetReadCallback() {
      int index = 0;

      @Override
      public void read(ResultSet rs) throws SQLException {
        if (index < 100) {
          assertEquals(rs.getString(FieldsMappings.CONNECT_TOPIC_COLUMN), topic);
          assertEquals(rs.getInt(FieldsMappings.CONNECT_PARTITION_COLUMN), partition);
          assertEquals(rs.getLong(FieldsMappings.CONNECT_OFFSET_COLUMN), index);
          assertEquals(rs.getString("firstName"), fName1);
          assertEquals(rs.getString("lastName"), lName1);
          assertEquals(rs.getBoolean("bool"), bool1);
          assertEquals(rs.getShort("short"), s1);
          assertEquals(rs.getByte("byte"), b1);
          assertEquals(rs.getLong("long"), l1);
          assertEquals(Float.compare(rs.getFloat("float"), f1), 0);
          assertEquals(Double.compare(rs.getDouble("double"), d1), 0);
          assertTrue(Arrays.equals(rs.getBytes("bytes"), bs1));
          assertEquals(rs.getInt("age"), age1);
        } else {
          throw new RuntimeException(String.format("%d is too high", index));
        }
        index++;
      }

    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);
  }


  @Test
  public void handleNewUpstreamAddition() throws SQLException {
    String tableName = "column_added_scenario2";
    String createTable = "CREATE TABLE " + tableName + " (" +
                         "    firstName  TEXT," +
                         "    lastName  TEXT," +
                         "    age INTEGER," +
                         "    PRIMARY KEY(firstName, lastName)" +
                         ");";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable);

    Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA);

    final String fName1 = "Alex";
    final String lName1 = "Smith";
    final int age1 = 21;

    Struct struct1 = new Struct(schema)
        .put("firstName", fName1)
        .put("lastName", lName1)
        .put("age", age1);

    final String topic = "topic";
    final int partition = 2;

    QueryBuilder queryBuilder = new InsertQueryBuilder(new SQLiteDialect());
    Map<String, DataExtractorWithQueryBuilder> map = new HashMap<>();
    Map<String, FieldAlias> fields = new HashMap<>();
    map.put(topic.toLowerCase(),
            new DataExtractorWithQueryBuilder(
                queryBuilder,
                new RecordDataExtractor(new FieldsMappings(tableName, topic, true, InsertModeEnum.INSERT, fields, false, true, false))));

    List<DbTable> dbTables = Collections.emptyList();
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    ConnectionProvider connectionProvider = new ConnectionProvider(SQL_LITE_URI, null, null, 5, 100);

    Database executor = new Database(
        Collections.singleton(tableName),
        Collections.singleton(tableName), //allow auto evolution
        dbMetadata,
        new SQLiteDialect(),
        1);

    JdbcDbWriter writer = new JdbcDbWriter(
        connectionProvider,
        new PreparedStatementContextIterable(map, 1000),
        new ThrowErrorHandlingPolicy(),
        executor,
        10);

    writer.write(Collections.singleton(
        new SinkRecord(topic, partition, null, null, schema, struct1, 0)
    ));

    String query = "SELECT * FROM " + tableName + " ORDER BY firstName";
    SqlLiteHelper.ResultSetReadCallback callback = new SqlLiteHelper.ResultSetReadCallback() {

      @Override
      public void read(ResultSet rs) throws SQLException {
        assertEquals(rs.getString("firstName"), fName1);
        assertEquals(rs.getString("lastName"), lName1);
        assertEquals(rs.getInt("age"), age1);
      }
    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);

    // SqlLiteHelper.execute(SQL_LITE_URI, "ALTER TABLE " + tableName + " ADD COLUMN salary REAL NULL;");

    Schema schema2 = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .field("salary", Schema.OPTIONAL_FLOAT64_SCHEMA);

    final String fName2 = "Jane";
    final String lName2 = "Wood";
    final int age2 = 28;
    final double salary2 = 1956.15;
    Struct struct2 = new Struct(schema2)
        .put("firstName", fName2)
        .put("lastName", lName2)
        .put("age", age2)
        .put("salary", salary2);

    writer.write(Collections.singleton(
        new SinkRecord(topic, partition, null, null, schema2, struct2, 0)
    ));

    new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {
        assertEquals(rs.getString("firstName"), fName2);
        assertEquals(rs.getString("lastName"), lName2);
        assertEquals(rs.getInt("age"), age2);
        assertTrue(Double.compare(rs.getDouble("salary"), salary2) == 0);
      }
    };
  }
}
