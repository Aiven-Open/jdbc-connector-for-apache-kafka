package com.datamountaineer.streamreactor.connect.jdbc.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class SqlLiteHelper {
  private static final Logger logger = LoggerFactory.getLogger(SqlLiteHelper.class);

  public static void createTable(final String uri, final String createSql) throws SQLException {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = DriverManager.getConnection(uri);
      connection.setAutoCommit(false);
      stmt = connection.createStatement();
      stmt.executeUpdate(createSql);
      connection.commit();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.getMessage(), e);
        }
      }
    }
  }

  public static void deleteTable(final String uri, final String table) throws SQLException {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = DriverManager.getConnection(uri);
      connection.setAutoCommit(false);
      stmt = connection.createStatement();
      stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
      connection.commit();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.getMessage(), e);
        }
      }
    }

    //random errors of table not being available happens in the unit tests
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void select(final String uri, final String query, final ResultSetReadCallback callback) throws SQLException {
    Connection connection = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      connection = DriverManager.getConnection(uri);

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
          logger.error(e.getMessage(), e);
        }
      }
    }
  }

  public static void execute(String uri, String query) throws SQLException {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = DriverManager.getConnection(uri);
      connection.setAutoCommit(false);

      stmt = connection.createStatement();
      stmt.execute(query);
      connection.commit();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.getMessage(), e);
        }
      }
    }
  }

  public interface ResultSetReadCallback {
    void read(final ResultSet rs) throws SQLException;
  }
}
