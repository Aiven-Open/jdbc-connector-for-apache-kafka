package io.confluent.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class SqlLiteHelper {

  public static void createTable(final String uri, final String createSql) throws SQLException {
    execute(uri, createSql);
  }

  public static void deleteTable(final String uri, final String table) throws SQLException {
    execute(uri, "DROP TABLE IF EXISTS " + table);

    //random errors of table not being available happens in the unit tests
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void select(final String uri, final String query, final ResultSetReadCallback callback) throws SQLException {
    try (Connection connection = DriverManager.getConnection(uri)) {
      connection.setAutoCommit(false);
      try (Statement stmt = connection.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            callback.read(rs);
          }
        }
      }
    }
  }

  public static void execute(String uri, String query) throws SQLException {
    try (Connection connection = DriverManager.getConnection(uri)) {
      connection.setAutoCommit(false);
      try (Statement stmt = connection.createStatement()) {
        stmt.executeUpdate(query);
        connection.commit();
      }
    }
  }

  public interface ResultSetReadCallback {
    void read(final ResultSet rs) throws SQLException;
  }
}
