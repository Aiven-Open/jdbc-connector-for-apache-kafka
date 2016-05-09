/**
 * Copyright 2015 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.ResultSet;

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