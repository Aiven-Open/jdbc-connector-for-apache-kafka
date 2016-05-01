package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;


import com.datamountaineer.streamreactor.connect.config.PayloadFields;
import com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcDriverLoader;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.ErrorPolicyEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.SqlLiteHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcDbWriterTest {

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
    public void writerShouldUseBatching() {
        JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI,
                "tableA",
                new PayloadFields(true, new HashMap<String, String>()),
                true,
                ErrorPolicyEnum.NOOP);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getStatementBuilder().getClass(), BatchedPreparedStatementBuilder.class);
    }

    @Test
    public void writerShouldUseNonBatching() {

        JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI,
                "tableA",
                new PayloadFields(true, new HashMap<String, String>()),
                false,
                ErrorPolicyEnum.NOOP);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getStatementBuilder().getClass(), SinglePreparedStatementBuilder.class);
    }


    @Test
    public void writerShouldUseNoopForErrorHandling() {

        JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI,
                "tableA",
                new PayloadFields(true, Maps.<String, String>newHashMap()),
                true,
                ErrorPolicyEnum.NOOP);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getErrorHandlingPolicy().getClass(), NoopErrorHandlingPolicy.class);
    }

    @Test
    public void writerShouldUseThrowForErrorHandling() {

        JdbcSinkSettings settings = new JdbcSinkSettings(SQL_LITE_URI, "tableA",
                new PayloadFields(true, new HashMap<String, String>()),
                true,
                ErrorPolicyEnum.THROW);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getErrorHandlingPolicy().getClass(), ThrowErrorHandlingPolicy.class);
    }


    @Test
    public void errorPolicyShouldHideTheException() throws SQLException {
        PreparedStatementBuilder builder = mock(PreparedStatementBuilder.class);
        Exception ex = new SQLException("some error description");

        Collection<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "test1", Schema.STRING_SCHEMA, "value", 0));

        when(builder.build(any(records.getClass()), any(Connection.class))).thenThrow(ex);
        JdbcDbWriter writer = new JdbcDbWriter(SQL_LITE_URI, builder, new NoopErrorHandlingPolicy());
        writer.write(records);
    }


    @Test(expected = RuntimeException.class)
    public void errorPolicyShouldThrowTheException() throws SQLException {
        PreparedStatementBuilder builder = mock(PreparedStatementBuilder.class);
        Exception ex = new SQLException("some error description");
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "test1", Schema.STRING_SCHEMA, "value", 0));

        when(builder.build(any(records.getClass()), any(Connection.class))).thenThrow(ex);
        JdbcDbWriter writer = new JdbcDbWriter(SQL_LITE_URI, builder, new ThrowErrorHandlingPolicy());
        writer.write(records);
    }

    @Test
    public void handleSingleStatementPerRecord() throws SQLException {
        String tableName = "single_statement_test";
        String createTable = "CREATE TABLE " + tableName + "(" +
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


        JdbcDbWriter writer = new JdbcDbWriter(SQL_LITE_URI,
                new SinglePreparedStatementBuilder(tableName, new StructFieldsDataExtractor(true, new HashMap<String, String>())),
                new ThrowErrorHandlingPolicy());

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
    public void handleSingleStatementPerRecordInsertingSameRecord100Times() throws SQLException {
        String tableName = "single_statement_test_100";
        String createTable = "CREATE TABLE " + tableName + "(" +
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


        JdbcDbWriter writer = new JdbcDbWriter(SQL_LITE_URI,
                new SinglePreparedStatementBuilder(tableName, new StructFieldsDataExtractor(true, new HashMap<String, String>())),
                new ThrowErrorHandlingPolicy());

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
        String createTable = "CREATE TABLE " + tableName + "(" +
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


        JdbcDbWriter writer = new JdbcDbWriter(SQL_LITE_URI,
                new BatchedPreparedStatementBuilder(tableName, new StructFieldsDataExtractor(true, new HashMap<String, String>())),
                new ThrowErrorHandlingPolicy());

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
        String createTable = "CREATE TABLE " + tableName + "(" +
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


        JdbcDbWriter writer = new JdbcDbWriter(SQL_LITE_URI,
                new BatchedPreparedStatementBuilder(tableName, new StructFieldsDataExtractor(true, new HashMap<String, String>())),
                new ThrowErrorHandlingPolicy());

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

}
