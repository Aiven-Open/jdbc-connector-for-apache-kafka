package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadataProvider;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTableColumn;
import com.datamountaineer.streamreactor.connect.jdbc.common.HikariHelper;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.DbDialect;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.OracleDialect;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.SQLiteDialect;
import com.datamountaineer.streamreactor.connect.jdbc.sink.DatabaseChangesExecutor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.SqlLiteHelper;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.ErrorPolicyEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.InsertModeEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariDataSource;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcDbWriterTest {

  private static final String DB_FILE = "test_db_writer_sqllite-jdbc-writer-test.db";
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
  public void writerShouldUseBatching() throws RestClientException, SQLException, IOException {
    List<FieldsMappings> fieldsMappingsList =
            Lists.newArrayList(new FieldsMappings("tableA", "topica", true, new HashMap<String, FieldAlias>()));


    JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI,
            null,
            null,
            fieldsMappingsList,
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.INSERT,
            10,
            "", JdbcSinkConfig.DEFAULT_PK_COL_NAME_VALUE);

    List<DbTableColumn> columns = Lists.newArrayList(
            new DbTableColumn("col1", true, false, 1),
            new DbTableColumn("col2", false, true, 1)
    );
    final DbTable tableA = new DbTable("tableA", columns);

    DatabaseMetadataProvider provider = mock(DatabaseMetadataProvider.class);
    when(provider.get(any(HikariDataSource.class)))
            .thenReturn(new DatabaseMetadata(null, Lists.newArrayList(tableA)));
    JdbcDbWriter writer = JdbcDbWriter.from(settings, provider);

    assertEquals(writer.getStatementBuilder().getClass(), BatchedPreparedStatementBuilder.class);
  }

  @Test
  public void writerShouldUseNonBatching() throws RestClientException, SQLException, IOException {
    List<FieldsMappings> fieldsMappingsList =
            Lists.newArrayList(new FieldsMappings("tableA", "topicA", true, new HashMap<String, FieldAlias>()));

    JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI,
            null,
            null,
            fieldsMappingsList,
            false,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.INSERT,
            10, "", JdbcSinkConfig.DEFAULT_PK_COL_NAME_VALUE);

    List<DbTableColumn> columns = Lists.newArrayList(
            new DbTableColumn("col1", true, false, 1),
            new DbTableColumn("col2", false, true, 1));
    final DbTable tableA = new DbTable("tableA", columns);

    DatabaseMetadataProvider provider = mock(DatabaseMetadataProvider.class);
    when(provider.get(any(HikariDataSource.class)))
            .thenReturn(new DatabaseMetadata(null, Lists.newArrayList(tableA)));
    JdbcDbWriter writer = JdbcDbWriter.from(settings, provider);

    assertEquals(writer.getStatementBuilder().getClass(), SinglePreparedStatementBuilder.class);
  }


  @Test
  public void writerShouldUseNoopForErrorHandling() throws SQLException, IOException, RestClientException {
    List<FieldsMappings> fieldsMappingsList =
            Lists.newArrayList(new FieldsMappings("tableA", "tableA", true, new HashMap<String, FieldAlias>()));

    JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI,
            null,
            null,
            fieldsMappingsList,
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.INSERT,
            10, "", JdbcSinkConfig.DEFAULT_PK_COL_NAME_VALUE
    );

    List<DbTableColumn> columns = Lists.newArrayList(
            new DbTableColumn("col1", true, false, 1),
            new DbTableColumn("col2", false, true, 1));
    final DbTable tableA = new DbTable("tableA", columns);

    DatabaseMetadataProvider provider = mock(DatabaseMetadataProvider.class);
    when(provider.get(any(HikariDataSource.class)))
            .thenReturn(new DatabaseMetadata(null, Lists.newArrayList(tableA)));
    JdbcDbWriter writer = JdbcDbWriter.from(settings, provider);

    assertEquals(writer.getErrorHandlingPolicy().getClass(), NoopErrorHandlingPolicy.class);
  }

  @Test
  public void writerShouldUseThrowForErrorHandling() throws SQLException, IOException, RestClientException {
    List<FieldsMappings> fieldsMappingsList =
            Lists.newArrayList(new FieldsMappings("tableA", "topicA", true, new HashMap<String, FieldAlias>()));

    JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI,
            null,
            null,
            fieldsMappingsList,
            true,
            ErrorPolicyEnum.THROW,
            InsertModeEnum.INSERT,
            10, "", JdbcSinkConfig.DEFAULT_PK_COL_NAME_VALUE);

    List<DbTableColumn> columns = Lists.newArrayList(
            new DbTableColumn("col1", true, false, 1),
            new DbTableColumn("col2", false, true, 1));
    final DbTable tableA = new DbTable("tableA", columns);

    DatabaseMetadataProvider provider = mock(DatabaseMetadataProvider.class);
    when(provider.get(any(HikariDataSource.class)))
            .thenReturn(new DatabaseMetadata(null, Lists.newArrayList(tableA)));
    JdbcDbWriter writer = JdbcDbWriter.from(settings, provider);

    assertEquals(writer.getErrorHandlingPolicy().getClass(), ThrowErrorHandlingPolicy.class);
  }

  @Test(expected = RuntimeException.class)
  public void errorPolicyShouldThrowTheException() throws SQLException {
    PreparedStatementBuilder builder = mock(PreparedStatementBuilder.class);
    Exception ex = new SQLException("some error description");
    Collection<SinkRecord> records = new ArrayList<>();
    records.add(new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "test1", Schema.STRING_SCHEMA, "value", 0));

    when(builder.build(any(records.getClass()))).thenThrow(ex);
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, Lists.<DbTable>newArrayList());
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            new OracleDialect(),
            1);

    JdbcDbWriter writer = new JdbcDbWriter(ds, builder, new ThrowErrorHandlingPolicy(), executor, 10);
    writer.write(records);
  }

  @Test
  public void handleSingleStatementPerRecord() throws SQLException, InterruptedException {
    String tableName1 = "single_statement_test_1";
    String createTable1 = "CREATE TABLE " + tableName1 + " (" +
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

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName1);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable1);

    String tableName2 = "single_statement_test_2";
    String createTable2 = "CREATE TABLE " + tableName2 + " (" +
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

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName2);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable2);

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

    String topic1 = "topic1";
    String topic2 = "topic2";
    int partition = 2;
    Collection<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic1, partition, null, null, schema, struct1, 1),
            new SinkRecord(topic2, partition, null, null, schema, struct2, 2));


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic1.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName1, topic1, true, new HashMap<String, FieldAlias>())));
    map.put(topic2.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName2, topic2, true, new HashMap<String, FieldAlias>())));

    Thread.sleep(1000);

    List<DbTable> dbTables = Lists.newArrayList(
            new DbTable(tableName1, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1),
                    new DbTableColumn("short", true, false, 1),
                    new DbTableColumn("long", true, false, 1),
                    new DbTableColumn("float", true, false, 1),
                    new DbTableColumn("double", true, false, 1),
                    new DbTableColumn("BLOB", true, false, 1)
            )),
            new DbTable(tableName2, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1),
                    new DbTableColumn("short", true, false, 1),
                    new DbTableColumn("long", true, false, 1),
                    new DbTableColumn("float", true, false, 1),
                    new DbTableColumn("double", true, false, 1),
                    new DbTableColumn("BLOB", true, false, 1)
            )));

    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            new SQLiteDialect(),
            1);
    JdbcDbWriter writer = new JdbcDbWriter(
            ds,
            new SinglePreparedStatementBuilder(map, new InsertQueryBuilder(new SQLiteDialect())),
            new ThrowErrorHandlingPolicy(),
            executor,
            10);

    writer.write(records);

    String query1 = "SELECT * FROM " + tableName1 + " WHERE firstName='" + fName1 + "'";
    SqlLiteHelper.ResultSetReadCallback callback1 = new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {
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
      }

    };

    SqlLiteHelper.select(SQL_LITE_URI, query1, callback1);

    String query2 = "SELECT * FROM " + tableName2 + " WHERE firstName='" + fName1 + "'";
    SqlLiteHelper.ResultSetReadCallback callback2 = new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {
        assertEquals(rs.getString("firstName"), fName2);
        assertEquals(rs.getString("lastName"), lName2);
        assertEquals(rs.getBoolean("bool"), bool2);
        assertEquals(rs.getObject("short"), null);
        assertEquals(rs.getByte("byte"), b2);
        assertEquals(rs.getLong("long"), l2);
        assertNull(rs.getObject("float"));
        assertEquals(Double.compare(rs.getDouble("double"), d2), 0);
        assertNull(rs.getBytes("bytes"));
        assertEquals(rs.getInt("age"), age2);
      }
    };

    SqlLiteHelper.select(SQL_LITE_URI, query2, callback2);

  }

  @Test
  public void handleSingleStatementPerRecordInsertingSameRecord100Times() throws SQLException {
    String tableName = "single_statement_test_100";
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

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable);

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

    String topic = "topic";
    int partition = 2;
    Collection<SinkRecord> records = Collections.nCopies(
            100,
            new SinkRecord(topic, partition, null, null, schema, struct1, 1));


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName, topic, true, new HashMap<String, FieldAlias>())));

    List<DbTable> dbTables = Lists.newArrayList(
            new DbTable(tableName, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1),
                    new DbTableColumn("short", true, false, 1),
                    new DbTableColumn("long", true, false, 1),
                    new DbTableColumn("float", true, false, 1),
                    new DbTableColumn("double", true, false, 1),
                    new DbTableColumn("BLOB", true, false, 1)
            )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            new SQLiteDialect(),
            1);
    JdbcDbWriter writer = new JdbcDbWriter(
            ds,
            new SinglePreparedStatementBuilder(map, new InsertQueryBuilder(new SQLiteDialect())),
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
        } else throw new RuntimeException(String.format("%d is too high", index));
        index++;
      }

    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);

  }

  @Test
  public void handleBatchedStatementPerRecordInsertingSameRecord100Times() throws SQLException {
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

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable);

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

    String topic = "topic";
    int partition = 2;
    Collection<SinkRecord> records = Collections.nCopies(
            100,
            new SinkRecord(topic, partition, null, null, schema, struct1, 1));

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    map.put(topic.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName, topic, true, new HashMap<String, FieldAlias>())));

    List<DbTable> dbTables = Lists.newArrayList(
            new DbTable(tableName, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1),
                    new DbTableColumn("short", true, false, 1),
                    new DbTableColumn("long", true, false, 1),
                    new DbTableColumn("float", true, false, 1),
                    new DbTableColumn("double", true, false, 1),
                    new DbTableColumn("BLOB", true, false, 1)
            )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            new SQLiteDialect(),
            1);
    JdbcDbWriter writer = new JdbcDbWriter(ds,
            new BatchedPreparedStatementBuilder(map, new InsertQueryBuilder(new SQLiteDialect())),
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
        } else throw new RuntimeException(String.format("%d is too high", index));
        index++;
      }

    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);

  }

  @Test
  public void handleBatchStatementPerRecord() throws SQLException {
    String tableName = "batched_statement_test";
    String createTable = "CREATE TABLE " + tableName + " (" +
            "    firstName  TEXT PRIMARY_KEY," +
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

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable);

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

    String topic = "topic";
    int partition = 2;
    Collection<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic, partition, null, null, schema, struct1, 1),
            new SinkRecord(topic, partition, null, null, schema, struct2, 2));


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    Map<String, FieldAlias> aliasMapPK = new HashMap<>();
    aliasMapPK.put("firstName", new FieldAlias("firstName", true));
    map.put(topic.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName, topic, true, aliasMapPK)));

    List<DbTable> dbTables = Lists.newArrayList(
            new DbTable(tableName, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1),
                    new DbTableColumn("short", true, false, 1),
                    new DbTableColumn("long", true, false, 1),
                    new DbTableColumn("float", true, false, 1),
                    new DbTableColumn("double", true, false, 1),
                    new DbTableColumn("BLOB", true, false, 1)
            )));

    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    DbDialect dbDialect = DbDialect.fromConnectionString(SQL_LITE_URI);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            dbDialect,
            1);
    JdbcDbWriter writer = new JdbcDbWriter(ds,
            new BatchedPreparedStatementBuilder(map, new UpsertQueryBuilder(dbDialect)),
            new ThrowErrorHandlingPolicy(),
            executor,
            10);

    writer.write(records);

    String query = "SELECT * FROM " + tableName + " ORDER BY firstName";
    SqlLiteHelper.ResultSetReadCallback callback = new SqlLiteHelper.ResultSetReadCallback() {
      int index = 0;

      @Override
      public void read(ResultSet rs) throws SQLException {
        if (index == 0) {
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
        } else if (index == 1) {
          assertEquals(rs.getString("firstName"), fName2);
          assertEquals(rs.getString("lastName"), lName2);
          assertEquals(rs.getBoolean("bool"), bool2);
          assertEquals(rs.getObject("short"), null);
          assertEquals(rs.getByte("byte"), b2);
          assertEquals(rs.getLong("long"), l2);
          assertNull(rs.getObject("float"));
          assertEquals(Double.compare(rs.getDouble("double"), d2), 0);
          assertNull(rs.getBytes("bytes"));
          assertEquals(rs.getInt("age"), age2);
        } else throw new RuntimeException(String.format("%d is too high", index));
        index++;
      }

    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);
  }


  @Test
  public void handleBatchedUpsert() throws SQLException {
    String tableName1 = "batched_upsert_test_1";
    String createTable1 = "CREATE TABLE " + tableName1 + " (" +
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

    String tableName2 = "batched_upsert_test_2";
    String createTable2 = "CREATE TABLE " + tableName2 + " (" +
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

    String topic1 = "topic1";
    String topic2 = "topic2";

    int partition = 2;
    Collection<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic1, partition, null, null, schema, struct1, 1),
            new SinkRecord(topic2, partition, null, null, schema, struct2, 2),
            new SinkRecord(topic1, partition, null, null, schema, struct1a, 3),
            new SinkRecord(topic2, partition, null, null, schema, struct2, 4));

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    Map<String, FieldAlias> aliasPKMap = new HashMap<>();

    aliasPKMap.put("firstName", new FieldAlias("firstName", true));
    aliasPKMap.put("lastName", new FieldAlias("lastName", true));
    map.put(topic1.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName1, topic1, true, aliasPKMap)));
    map.put(topic2.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName2, topic2, true, aliasPKMap)));

    DbDialect dbDialect = DbDialect.fromConnectionString(SQL_LITE_URI);
    List<DbTable> dbTables = Lists.newArrayList(
            new DbTable(tableName1, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1),
                    new DbTableColumn("short", true, false, 1),
                    new DbTableColumn("long", true, false, 1),
                    new DbTableColumn("float", true, false, 1),
                    new DbTableColumn("double", true, false, 1),
                    new DbTableColumn("BLOB", true, false, 1)
            )),
            new DbTable(tableName2, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1),
                    new DbTableColumn("short", true, false, 1),
                    new DbTableColumn("long", true, false, 1),
                    new DbTableColumn("float", true, false, 1),
                    new DbTableColumn("double", true, false, 1),
                    new DbTableColumn("BLOB", true, false, 1)
            )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);

    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            dbDialect,
            1);
    JdbcDbWriter writer = new JdbcDbWriter(ds,
            new BatchedPreparedStatementBuilder(map, new UpsertQueryBuilder(dbDialect)),
            new ThrowErrorHandlingPolicy(),
            executor,
            10);

    writer.write(records);

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


    query = "SELECT * FROM " + tableName2 + " WHERE firstName='" + fName2 + "' and lastName='" + lName2 + "'";


    SqlLiteHelper.select(SQL_LITE_URI, query, new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {

        assertEquals(rs.getBoolean("bool"), bool2);
        assertEquals(rs.getShort("short"), 0);
        assertEquals(rs.getByte("byte"), b2);
        assertEquals(rs.getLong("long"), l2);
        assertTrue(Float.compare(rs.getFloat("float"), 0) == 0);
        assertEquals(Double.compare(rs.getDouble("double"), d2), 0);
        assertEquals(rs.getInt("age"), age2);
      }
    });
  }


  @Test
  public void handleBatchStatementPerRecordWhenThePayloadHasMoreFieldsThanTheTable() throws SQLException {
    String tableName = "batched_statement_more_fields_than_columns_test";
    String createTable = "CREATE TABLE " + tableName + " (" +
            "    firstName  TEXT PRIMARY_KEY," +
            "    lastName  TEXT," +
            "    age INTEGER," +
            "    bool  NUMERIC," +
            "    byte  INTEGER" +
            ");";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable);

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

    String topic = "topic";
    int partition = 2;
    Collection<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic, partition, null, null, schema, struct1, 1),
            new SinkRecord(topic, partition, null, null, schema, struct2, 2));


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("firstName", new FieldAlias("firstName", true));
    aliasMap.put("lastName", new FieldAlias("lastName", false));
    aliasMap.put("age", new FieldAlias("age", false));
    aliasMap.put("bool", new FieldAlias("bool", false));
    aliasMap.put("byte", new FieldAlias("byte", false));

    map.put(topic.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName, topic, false, aliasMap)));

    List<DbTable> dbTables = Lists.newArrayList(
            new DbTable(tableName, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1),
                    new DbTableColumn("bool", true, false, 1),
                    new DbTableColumn("byte", true, false, 1)
            )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    DbDialect dbDialect = DbDialect.fromConnectionString(SQL_LITE_URI);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            dbDialect,
            1);
    JdbcDbWriter writer = new JdbcDbWriter(ds,
            new BatchedPreparedStatementBuilder(map, new UpsertQueryBuilder(dbDialect)),
            new ThrowErrorHandlingPolicy(),
            executor,
            10);

    writer.write(records);

    String query = "SELECT * FROM " + tableName + " ORDER BY firstName";
    SqlLiteHelper.ResultSetReadCallback callback = new SqlLiteHelper.ResultSetReadCallback() {
      int index = 0;

      @Override
      public void read(ResultSet rs) throws SQLException {
        if (index == 0) {
          assertEquals(rs.getString("firstName"), fName1);
          assertEquals(rs.getString("lastName"), lName1);
          assertEquals(rs.getBoolean("bool"), bool1);
          assertEquals(rs.getByte("byte"), b1);
          assertEquals(rs.getInt("age"), age1);
        } else if (index == 1) {
          assertEquals(rs.getString("firstName"), fName2);
          assertEquals(rs.getString("lastName"), lName2);
          assertEquals(rs.getBoolean("bool"), bool2);
          assertEquals(rs.getByte("byte"), b2);
          assertEquals(rs.getInt("age"), age2);
        } else throw new RuntimeException(String.format("%d is too high", index));
        index++;
      }

    };

    SqlLiteHelper.select(SQL_LITE_URI, query, callback);
  }

  @Test
  public void handleSingleStatementPerRecordWhenPayloadHasMoreFieldsThanColumnsMapped() throws SQLException {
    String tableName1 = "single_statement_test_with_fields_more_than_columns_1";
    String createTable1 = "CREATE TABLE " + tableName1 + " (" +
            "    firstName  TEXT," +
            "    lastName  TEXT," +
            "    age INTEGER" +
            ");";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName1);
    SqlLiteHelper.createTable(SQL_LITE_URI, createTable1);

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


    String topic1 = "topic1";
    int partition = 2;
    Collection<SinkRecord> records = Lists.newArrayList(
            new SinkRecord(topic1, partition, null, null, schema, struct1, 1));


    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("firstName", new FieldAlias("firstName", false));
    aliasMap.put("lastName", new FieldAlias("lastName", false));
    aliasMap.put("age", new FieldAlias("age", false));
    map.put(topic1.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName1, topic1, false, aliasMap)));

    List<DbTable> dbTables = Lists.newArrayList(
            new DbTable(tableName1, Lists.<DbTableColumn>newArrayList(
                    new DbTableColumn("firstName", true, false, 1),
                    new DbTableColumn("lastName", true, false, 1),
                    new DbTableColumn("age", false, false, 1)
            )));

    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(),
            dbMetadata,
            new SQLiteDialect(),
            1);

    JdbcDbWriter writer = new JdbcDbWriter(ds,
            new SinglePreparedStatementBuilder(map, new InsertQueryBuilder(new SQLiteDialect())),
            new ThrowErrorHandlingPolicy(),
            executor,
            10);

    writer.write(records);

    String query1 = "SELECT * FROM " + tableName1 + " WHERE firstName='" + fName1 + "'";
    SqlLiteHelper.ResultSetReadCallback callback1 = new SqlLiteHelper.ResultSetReadCallback() {
      @Override
      public void read(ResultSet rs) throws SQLException {
        assertEquals(fName1, rs.getString("firstName"));
        assertEquals(lName1, rs.getString("lastName"));
        assertEquals(age1, rs.getInt("age"));
      }

    };

    SqlLiteHelper.select(SQL_LITE_URI, query1, callback1);
  }
}
