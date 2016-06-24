package com.datamountaineer.streamreactor.connect.jdbc.sink;


import com.datamountaineer.streamreactor.connect.jdbc.ConnectionProvider;
import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTableColumn;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.DbDialect;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.SQLiteDialect;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.confluent.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatabaseTest {

  private static final String DB_FILE = "test_database_changes_executor.sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;

  private final ConnectionProvider connectionProvider = new ConnectionProvider(SQL_LITE_URI, null, null, 100, 100);

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
  public void createAnInstance() {
    new Database(new HashSet<String>(),
            new HashSet<String>(),
            new DatabaseMetadata(null, new ArrayList<DbTable>()),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTablesAllowingAutoCreateIsNull() {
    new Database(null,
            new HashSet<String>(),
            new DatabaseMetadata(null, new ArrayList<DbTable>()),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTablesAllowingSchemaEvolutionIsNull() {
    new Database(new HashSet<String>(),
            null,
            new DatabaseMetadata(null, new ArrayList<DbTable>()),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);
  }

  @Test
  public void createTheNewTables() throws SQLException {

    String tableName1 = "tableA";
    String tableName2 = "tableB";
    Database changesExecutor = new Database(Sets.newHashSet(tableName1, tableName2),
            new HashSet<String>(),
            new DatabaseMetadata(null, new ArrayList<DbTable>()),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);

    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put(tableName1, Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", false),
            new SinkRecordField(Schema.Type.INT8, "col3", false),
            new SinkRecordField(Schema.Type.INT64, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false)
    ));

    map.put(tableName2, Lists.newArrayList(
            new SinkRecordField(Schema.Type.STRING, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", true),
            new SinkRecordField(Schema.Type.FLOAT32, "col3", false),
            new SinkRecordField(Schema.Type.BYTES, "col4", false)
    ));

    Connection connection = null;
    try {
      connection = connectionProvider.getConnection();
      changesExecutor.update(map, connection);
      assertTrue(DatabaseMetadata.tableExists(connection, tableName1));
      assertTrue(DatabaseMetadata.tableExists(connection, tableName2));

      DbTable table1 = DatabaseMetadata.getTableMetadata(connection, tableName1);
      assertEquals(tableName1, table1.getName());

      Map<String, DbTableColumn> columnMap = table1.getColumns();
      for (int i = 1; i <= 5; ++i) {
        assertTrue(columnMap.containsKey("col" + i));
      }

      assertTrue(columnMap.get("col1").isPrimaryKey());
      assertFalse(columnMap.get("col1").allowsNull());

      assertFalse(columnMap.get("col2").isPrimaryKey());
      assertTrue(columnMap.get("col2").allowsNull());

      assertFalse(columnMap.get("col3").isPrimaryKey());
      assertTrue(columnMap.get("col3").allowsNull());

      assertFalse(columnMap.get("col4").isPrimaryKey());
      assertTrue(columnMap.get("col4").allowsNull());

      assertFalse(columnMap.get("col5").isPrimaryKey());
      assertTrue(columnMap.get("col5").allowsNull());

      DbTable table2 = DatabaseMetadata.getTableMetadata(connection, tableName2);
      assertEquals(tableName2, table2.getName());

      columnMap = table2.getColumns();
      for (int i = 1; i <= 4; ++i) {
        assertTrue(columnMap.containsKey("col" + i));
      }

      //weird sqlite shows col1 as nonpk and only col2 as pk
      assertFalse(columnMap.get("col1").isPrimaryKey());
      assertFalse(columnMap.get("col1").allowsNull());

      assertTrue(columnMap.get("col2").isPrimaryKey());
      assertFalse(columnMap.get("col2").allowsNull());

      assertFalse(columnMap.get("col3").isPrimaryKey());
      assertTrue(columnMap.get("col3").allowsNull());

      assertFalse(columnMap.get("col4").isPrimaryKey());
      assertTrue(columnMap.get("col4").allowsNull());

    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test(expected = ConfigException.class)
  public void throwAnExceptionWhenForANewTableToCreateWhichDoesNotAllowAutoCreation() throws SQLException {
    Database changesExecutor = new Database(new HashSet<String>(),
            new HashSet<String>(),
            new DatabaseMetadata(null, new ArrayList<DbTable>()),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);

    String tableName = "tableA";
    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put(tableName, Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", false),
            new SinkRecordField(Schema.Type.INT8, "col3", false),
            new SinkRecordField(Schema.Type.INT64, "col3", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col4", false)
    ));

    try (Connection connection = connectionProvider.getConnection())  {
      changesExecutor.update(map, connection);
    }
  }

  @Test
  public void amendTables() throws SQLException {

    String tableName1 = "tableAa";
    String tableName2 = "tableBb";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName2);
    SqlLiteHelper.createTable(SQL_LITE_URI, "CREATE TABLE " + tableName2 + " (\n" +
            "col1 TEXT NOT NULL ,\n" +
            "col2 TEXT NOT NULL ,\n" +
            "col3 REAL NULL," +
            "PRIMARY KEY(col1,col2));");

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName1);
    SqlLiteHelper.createTable(SQL_LITE_URI, "CREATE TABLE " + tableName1 + " (\n" +
            "col1 NUMERIC NOT NULL ,\n" +
            "col2 TEXT NULL,\n" +
            "col3 NUMERIC NULL," +
            "PRIMARY KEY(col1));");


    Database changesExecutor = new Database(new HashSet<String>(),
            Sets.newHashSet(tableName1, tableName2),
            new DatabaseMetadata(null, DatabaseMetadata.getTableMetadata(connectionProvider)),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);

    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put(tableName1, Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", false),
            new SinkRecordField(Schema.Type.INT8, "col3", false),
            new SinkRecordField(Schema.Type.INT64, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false)
    ));

    map.put(tableName2, Lists.newArrayList(
            new SinkRecordField(Schema.Type.STRING, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", true),
            new SinkRecordField(Schema.Type.FLOAT32, "col3", false),
            new SinkRecordField(Schema.Type.BYTES, "col4", false)
    ));

    Connection connection = null;
    try {
      connection = connectionProvider.getConnection();
      changesExecutor.update(map, connection);

      assertTrue(DatabaseMetadata.tableExists(connection, tableName1));
      assertTrue(DatabaseMetadata.tableExists(connection, tableName2));

      DbTable table1 = DatabaseMetadata.getTableMetadata(connection, tableName1);
      assertEquals(tableName1, table1.getName());

      Map<String, DbTableColumn> columnMap = table1.getColumns();

      assertTrue(columnMap.containsKey("col4"));
      assertTrue(columnMap.containsKey("col5"));

      assertTrue(columnMap.get("col1").isPrimaryKey());
      assertFalse(columnMap.get("col1").allowsNull());

      assertFalse(columnMap.get("col2").isPrimaryKey());
      assertTrue(columnMap.get("col2").allowsNull());

      assertFalse(columnMap.get("col3").isPrimaryKey());
      assertTrue(columnMap.get("col3").allowsNull());

      assertFalse(columnMap.get("col4").isPrimaryKey());
      assertTrue(columnMap.get("col4").allowsNull());

      assertFalse(columnMap.get("col5").isPrimaryKey());
      assertTrue(columnMap.get("col5").allowsNull());

      DbTable table2 = DatabaseMetadata.getTableMetadata(connection, tableName2);
      assertEquals(tableName2, table2.getName());

      columnMap = table2.getColumns();
      assertTrue(columnMap.containsKey("col4"));


      //weird sqlite shows col1 as nonpk and only col2 as pk
      assertFalse(columnMap.get("col1").isPrimaryKey());
      assertFalse(columnMap.get("col1").allowsNull());

      assertTrue(columnMap.get("col2").isPrimaryKey());
      assertFalse(columnMap.get("col2").allowsNull());

      assertFalse(columnMap.get("col3").isPrimaryKey());
      assertTrue(columnMap.get("col3").allowsNull());

      assertFalse(columnMap.get("col4").isPrimaryKey());
      assertTrue(columnMap.get("col4").allowsNull());

    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void handleTheScenarioWhereTheTableHasBeenAlreadyCreated() throws SQLException {

    String tableName1 = "tableA1";
    Database changesExecutor = new Database(Sets.newHashSet(tableName1),
            new HashSet<String>(),
            new DatabaseMetadata(null, new ArrayList<DbTable>()),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);

    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put(tableName1, Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", false),
            new SinkRecordField(Schema.Type.INT8, "col3", false),
            new SinkRecordField(Schema.Type.INT64, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false)
    ));


    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName1);
    SqlLiteHelper.createTable(SQL_LITE_URI, "CREATE TABLE " + tableName1 + " (\n" +
            "col1 NUMERIC NOT NULL ,\n" +
            "col2 TEXT NULL,\n" +
            "col3 NUMERIC NULL,\n" +
            "col4 NUMERIC NULL,\n" +
            "col5 REAL NULL,\n" +
            "PRIMARY KEY(col1));");

    Connection connection = null;
    try {
      connection = connectionProvider.getConnection();
      changesExecutor.update(map, connection);

      assertTrue(DatabaseMetadata.tableExists(connection, tableName1));

      DbTable table1 = DatabaseMetadata.getTableMetadata(connection, tableName1);
      assertEquals(tableName1, table1.getName());

      Map<String, DbTableColumn> columnMap = table1.getColumns();
      for (int i = 1; i <= 5; ++i) {
        assertTrue(columnMap.containsKey("col" + i));
      }

      assertTrue(columnMap.get("col1").isPrimaryKey());
      assertFalse(columnMap.get("col1").allowsNull());

      assertFalse(columnMap.get("col2").isPrimaryKey());
      assertTrue(columnMap.get("col2").allowsNull());

      assertFalse(columnMap.get("col3").isPrimaryKey());
      assertTrue(columnMap.get("col3").allowsNull());

      assertFalse(columnMap.get("col4").isPrimaryKey());
      assertTrue(columnMap.get("col4").allowsNull());

      assertFalse(columnMap.get("col5").isPrimaryKey());
      assertTrue(columnMap.get("col5").allowsNull());


    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void handleTheScenarioWhereTheTableHasBeenAlreadyCreatedButNotAllTheColumnsArePresent() throws SQLException {

    String tableName1 = "tableA11";
    Database changesExecutor = new Database(Sets.newHashSet(tableName1),
            new HashSet<String>(),
            new DatabaseMetadata(null, new ArrayList<DbTable>()),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);

    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put(tableName1, Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", false),
            new SinkRecordField(Schema.Type.INT8, "col3", false),
            new SinkRecordField(Schema.Type.INT64, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false)
    ));


    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName1);
    SqlLiteHelper.createTable(SQL_LITE_URI, "CREATE TABLE " + tableName1 + " (\n" +
            "col1 NUMERIC NOT NULL ,\n" +
            "col2 TEXT NULL,\n" +
            "col3 NUMERIC NULL,\n" +
            "PRIMARY KEY(col1));");

    Connection connection = null;
    try {
      connection = connectionProvider.getConnection();
      changesExecutor.update(map, connection);

      assertTrue(DatabaseMetadata.tableExists(connection, tableName1));

      DbTable table1 = DatabaseMetadata.getTableMetadata(connection, tableName1);
      assertEquals(tableName1, table1.getName());

      Map<String, DbTableColumn> columnMap = table1.getColumns();
      for (int i = 1; i <= 5; ++i) {
        assertTrue(columnMap.containsKey("col" + i));
      }

      assertTrue(columnMap.get("col1").isPrimaryKey());
      assertFalse(columnMap.get("col1").allowsNull());

      assertFalse(columnMap.get("col2").isPrimaryKey());
      assertTrue(columnMap.get("col2").allowsNull());

      assertFalse(columnMap.get("col3").isPrimaryKey());
      assertTrue(columnMap.get("col3").allowsNull());

      assertFalse(columnMap.get("col4").isPrimaryKey());
      assertTrue(columnMap.get("col4").allowsNull());

      assertFalse(columnMap.get("col5").isPrimaryKey());
      assertTrue(columnMap.get("col5").allowsNull());


    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void handleAmendmentsWhenTheTablesHaveBeenAmendedButSomeColumnsAreNotPresentYey() throws SQLException {

    String tableName1 = "tableAa1a";

    SqlLiteHelper.deleteTable(SQL_LITE_URI, tableName1);
    SqlLiteHelper.createTable(SQL_LITE_URI, "CREATE TABLE " + tableName1 + " (\n" +
            "col1 NUMERIC NOT NULL ,\n" +
            "col2 TEXT NULL,\n" +
            "col3 NUMERIC NULL," +
            "PRIMARY KEY(col1));");


    Database changesExecutor = new Database(new HashSet<String>(),
            Sets.newHashSet(tableName1),
            new DatabaseMetadata(null, DatabaseMetadata.getTableMetadata(connectionProvider)),
            DbDialect.fromConnectionString(SQL_LITE_URI),
            2);

    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put(tableName1, Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.STRING, "col2", false),
            new SinkRecordField(Schema.Type.INT8, "col3", false),
            new SinkRecordField(Schema.Type.INT64, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false)
    ));

    List<String> amendQuery = new SQLiteDialect().getAlterTable(tableName1, Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT64, "col4", false)
    ));

    SqlLiteHelper.execute(SQL_LITE_URI, amendQuery.get(0));

    //SQLite required delay
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      System.out.print(e.getMessage());
      throw new InterruptException(e);
    }
    Connection connection1 = null;
    try {
      connection1 = connectionProvider.getConnection();
      DbTable table = DatabaseMetadata.getTableMetadata(connection1, tableName1);
      assertTrue(table.getColumns().containsKey("col4"));
    } finally {
      if (connection1 != null) {
        connection1.close();
      }
    }

    Connection connection = null;
    try {
      connection = connectionProvider.getConnection();
      changesExecutor.update(map, connection);

      assertTrue(DatabaseMetadata.tableExists(connection, tableName1));

      DbTable table1 = DatabaseMetadata.getTableMetadata(connection, tableName1);
      assertEquals(tableName1, table1.getName());

      Map<String, DbTableColumn> columnMap = table1.getColumns();

      assertTrue(columnMap.containsKey("col4"));
      assertTrue(columnMap.containsKey("col5"));

      assertTrue(columnMap.get("col1").isPrimaryKey());
      assertFalse(columnMap.get("col1").allowsNull());

      assertFalse(columnMap.get("col2").isPrimaryKey());
      assertTrue(columnMap.get("col2").allowsNull());

      assertFalse(columnMap.get("col3").isPrimaryKey());
      assertTrue(columnMap.get("col3").allowsNull());

      assertFalse(columnMap.get("col4").isPrimaryKey());
      assertTrue(columnMap.get("col4").allowsNull());

      assertFalse(columnMap.get("col5").isPrimaryKey());
      assertTrue(columnMap.get("col5").allowsNull());

    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

}
