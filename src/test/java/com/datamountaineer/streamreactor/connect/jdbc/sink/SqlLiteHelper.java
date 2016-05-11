package com.datamountaineer.streamreactor.connect.jdbc.sink;

import java.sql.*;

public final class SqlLiteHelper {
  public static void createTable(final String uri, final String createSql) throws SQLException {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = DriverManager.getConnection(uri);

      stmt = connection.createStatement();
      stmt.executeUpdate(createSql);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {

        }
      }
    }
  }

  public static void select(final String uri, final String query, final ResultSetReadCallback callback) throws SQLException {
    Connection connection = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      connection = DriverManager.getConnection(uri);
      connection.setAutoCommit(false);

      stmt = connection.createStatement();
      rs = stmt.executeQuery(query);
      while (rs.next()) {
        callback.read(rs);
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {

        }
      }
    }
  }

  public static interface ResultSetReadCallback {
    void read(final ResultSet rs) throws SQLException;
  }
}
