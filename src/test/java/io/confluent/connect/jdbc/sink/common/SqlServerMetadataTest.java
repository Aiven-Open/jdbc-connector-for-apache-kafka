package io.confluent.connect.jdbc.sink.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class SqlServerMetadataTest {
  private final String URI = "jdbc:sqlserver://192.168.1.74:1433;DatabaseName=the_db";
  private final String user = "sa";
  private final String psw = "12345678";

  //@Test
  public void shouldReturnTrueIfTheTableExists() throws SQLException {
    String table = "Products";
    Connection connection = DriverManager.getConnection(URI, user, psw);
    try {
      assertTrue(DatabaseMetadata.tableExists(connection, table));
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  //@Test
  public void shouldReturnFalseIfTheTableDoesNotExists() throws SQLException {
    String table = "bibble";
    Connection connection = DriverManager.getConnection(URI, user, psw);
    try {
      assertFalse(DatabaseMetadata.tableExists(connection, table));
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  //@Test
  public void shouldReturnFalseEvenIfTheTableIsInAnotherDatabase() throws SQLException {
    String table = "tasks";
    Connection connection = DriverManager.getConnection(URI, user, psw);
    try {
      assertFalse(DatabaseMetadata.tableExists(connection, table));
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  //@Test
  public void shouldReturnTheTablesInfo() throws SQLException {
    String tableName = "Products";
    Connection connection = DriverManager.getConnection(URI, user, psw);
    try {
      DbTable table = DatabaseMetadata.getTableMetadata(connection, tableName);
      assertEquals(tableName, table.getName());
      Map<String, DbTableColumn> map = table.getColumns();
      assertEquals(4, map.size());
      assertTrue(map.containsKey("ProductID"));
      assertTrue(map.containsKey("ProductName"));
      assertTrue(map.containsKey("Price"));
      assertTrue(map.containsKey("ProductDescription"));

      assertTrue(map.get("ProductID").isPrimaryKey());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }


  /**
   * > CREATE DATABASE the_db;
   * > CREATE TABLE tutorials_tbl(
   tutorial_id INT NOT NULL AUTO_INCREMENT,
   tutorial_title VARCHAR(100) NOT NULL,
   tutorial_author VARCHAR(40) NOT NULL,
   submission_date DATE,
   PRIMARY KEY ( tutorial_id )
   );

   *
   * > create database other_db;
   * >CREATE TABLE IF NOT EXISTS tasks (
   task_id INT(11) NOT NULL AUTO_INCREMENT,
   subject VARCHAR(45) DEFAULT NULL,
   start_date DATE DEFAULT NULL,
   end_date DATE DEFAULT NULL,
   description VARCHAR(200) DEFAULT NULL,
   PRIMARY KEY (task_id)
   );
   *
   *
   *
   *
   */
}
