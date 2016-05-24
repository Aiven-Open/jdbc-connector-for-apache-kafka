package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.DatabaseChangesExecutor;
import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.common.HikariHelper;
import com.datamountaineer.streamreactor.connect.jdbc.sink.SqlLiteHelper;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.SQLiteDialect;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariDataSource;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
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
  public void handleSingleStatementPerRecordInsertingWithAutoCreatedColumnForPK() throws SQLException {
    String tableName = "single_100_auto_create_column";

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

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    Map<String, FieldAlias> fields = new HashMap<>();
    fields.put(FieldsMappings.CONNECT_AUTO_ID_COLUMN, new FieldAlias(FieldsMappings.CONNECT_AUTO_ID_COLUMN, true));
    map.put(topic.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName, topic, true, fields,
                    true, true)));

    List<DbTable> dbTables = Lists.newArrayList();
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(tableName),
            Sets.<String>newHashSet(),
            dbMetadata,
            new SQLiteDialect(),
            1);
    JdbcDbWriter writer = new JdbcDbWriter(
            ds,
            new SinglePreparedStatementBuilder(map, new InsertQueryBuilder()),
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
          assertEquals(rs.getString(FieldsMappings.CONNECT_AUTO_ID_COLUMN), String.format("%s.%s.%d", topic, partition, index));
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

    Map<String, StructFieldsDataExtractor> map = new HashMap<>();
    Map<String, FieldAlias> fields = new HashMap<>();
    fields.put(FieldsMappings.CONNECT_AUTO_ID_COLUMN, new FieldAlias(FieldsMappings.CONNECT_AUTO_ID_COLUMN, true));
    map.put(topic.toLowerCase(),
            new StructFieldsDataExtractor(new FieldsMappings(tableName, topic, true, fields,
                    true, true)));

    List<DbTable> dbTables = Lists.newArrayList();
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(tableName),
            Sets.<String>newHashSet(),
            dbMetadata,
            new SQLiteDialect(),
            1);
    JdbcDbWriter writer = new JdbcDbWriter(
            ds,
            new BatchedPreparedStatementBuilder(map, new InsertQueryBuilder()),
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
          assertEquals(rs.getString(FieldsMappings.CONNECT_AUTO_ID_COLUMN), String.format("%s.%s.%d", topic, partition, index));
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
  public void handleSingleStatementPerRecordInsertingSameRecord100Times() throws SQLException {
    String tableName = "single_statement_test_100";
    /*String createTable = "CREATE TABLE " + tableName + " (" +
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
*/

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
            new StructFieldsDataExtractor(new FieldsMappings(tableName, topic, true, new HashMap<String, FieldAlias>(),
                    true, true)));

    List<DbTable> dbTables = Lists.newArrayList();
    DatabaseMetadata dbMetadata = new DatabaseMetadata(null, dbTables);
    HikariDataSource ds = HikariHelper.from(SQL_LITE_URI, null, null);
    DatabaseChangesExecutor executor = new DatabaseChangesExecutor(
            ds,
            Sets.<String>newHashSet(tableName),
            Sets.<String>newHashSet(),
            dbMetadata,
            new SQLiteDialect(),
            1);
    JdbcDbWriter writer = new JdbcDbWriter(
            ds,
            new SinglePreparedStatementBuilder(map, new InsertQueryBuilder()),
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
}
