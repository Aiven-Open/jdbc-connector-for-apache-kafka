package io.confluent.connect.jdbc.sink.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PostgresMetadataTest {
  private final String URI = "jdbc:postgresql://localhost/the_db";
  private final String user = "postgres";
  private final String psw = "apassword";

  //@Test
  public void shouldReturnTrueIfTheTableExists() throws SQLException {
    String table = "playground";
    try (Connection connection = DriverManager.getConnection(URI, user, psw)) {
      assertTrue(DatabaseMetadata.tableExists(connection, table));
    }
  }

  //@Test
  public void shouldReturnFalseIfTheTableDoesNotExists() throws SQLException {
    String table = "bibble";
    try (Connection connection = DriverManager.getConnection(URI, user, psw)) {
      assertFalse(DatabaseMetadata.tableExists(connection, table));
    }
  }

  //@Test
  public void shouldReturnFalseEvenIfTheTableIsInAnotherDatabase() throws SQLException {
    String table = "company";
    try (Connection connection = DriverManager.getConnection(URI, user, psw)) {
      assertFalse(DatabaseMetadata.tableExists(connection, table));
    }
  }

  //@Test
  public void shouldReturnTheTablesInfo() throws SQLException {
    String tableName = "playground";
    try (Connection connection = DriverManager.getConnection(URI, user, psw)) {
      DbTable table = DatabaseMetadata.getTableMetadata(connection, tableName);
      assertEquals(tableName, table.getName());
      Map<String, DbTableColumn> map = table.getColumns();
      assertEquals(4, map.size());
      assertTrue(map.containsKey("equip_id"));
      assertTrue(map.containsKey("type"));
      assertTrue(map.containsKey("color"));
      assertTrue(map.containsKey("install_date"));

      assertTrue(map.get("equip_id").isPrimaryKey());
    }
  }

  /**
   * > create database the_db;
   * > \connect the_db
   * > CREATE TABLE playground (
   equip_id serial PRIMARY KEY,
   type varchar (50) NOT NULL,
   color varchar (25) NOT NULL,
   install_date date
   );

   *
   * > create database other_db;
   * > \connect other_db;
   * >CREATE TABLE COMPANY(
   ID INT PRIMARY KEY     NOT NULL,
   NAME           TEXT    NOT NULL,
   AGE            INT     NOT NULL,
   ADDRESS        CHAR(50),
   SALARY         REAL
   );
   *
   *
   *
   *
   */
}
