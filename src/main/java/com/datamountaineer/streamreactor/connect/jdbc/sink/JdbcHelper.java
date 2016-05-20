/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;


import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

class JdbcHelper {

  private static final Logger logger = LoggerFactory.getLogger(JdbcHelper.class);

  /**
   * Returns the database for the current connection
   *
   * @param connection - The database URI
   * @param user       - The database user name
   * @param password   - The database password
   * @return The database name
   * @throws SQLException
   */
  public static String getDatabase(final String connection, final String user, final String password) throws SQLException {
    Connection con = null;

    try {
      if (user != null) {
        con = DriverManager.getConnection(connection, user, password);
      } else {
        con = DriverManager.getConnection(connection);
      }

      return con.getCatalog();
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (Throwable t) {
          logger.error(t.getMessage(), t);
        }
      }
    }
  }

  /**
   * Creates a DatabaseMetadata instance for the tables given
   *
   * @param uri
   * @param user
   * @param password
   * @param tables
   * @return The database metadata
   */
  public static DatabaseMetadata getDatabaseMetadata(final String uri,
                                                     final String user,
                                                     final String password,
                                                     final Set<String> tables) {
    ParameterValidator.notNullOrEmpty(uri, "uri");
    ParameterValidator.notNull(tables, "tables");
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("<tables> parameter is empty");
    }
    Connection connection = null;
    try {
      if (user != null) {
        connection = DriverManager.getConnection(uri, user, password);
      } else {
        connection = DriverManager.getConnection(uri);
      }

      final String catalog = connection.getCatalog();
      final DatabaseMetaData dbMetadata = connection.getMetaData();

      final List<DbTable> dbTables = Lists.newArrayList();
      for (final String table : tables) {
        final List<DbTableColumn> columns = getTableColumns(catalog, table, dbMetadata);
        dbTables.add(new DbTable(table, columns));
      }

      return new DatabaseMetadata(catalog, dbTables);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Throwable t) {
          logger.error(t.getMessage(), t);
        }
      }
    }
  }


  /***
   * Returns the tables information
   *
   * @param uri
   * @param user
   * @param password
   * @return
   */
  public static List<DbTable> getTablesMetadata(final String uri, final String user, final String password) {
    Connection connection = null;
    try {
      if (user != null) {
        connection = DriverManager.getConnection(uri, user, password);
      } else {
        connection = DriverManager.getConnection(uri);
      }

      final String catalog = connection.getCatalog();
      final DatabaseMetaData dbMetadata = connection.getMetaData();
      ResultSet tablesRs = dbMetadata.getTables(catalog, null, null, new String[]{"TABLE"});

      final List<DbTable> tables = new ArrayList<>();
      while (tablesRs.next()) {
        final String tableName = tablesRs.getString("TABLE_NAME");
        final List<DbTableColumn> columns = getTableColumns(catalog, tableName, dbMetadata);

        tables.add(new DbTable(tableName, columns));
      }
      return tables;
    } catch (SQLException ex) {
      throw new RuntimeException("Sql exception occured.", ex);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Throwable t) {
          logger.error(t.getMessage(), t);
        }
      }
    }
  }

  private static List<DbTableColumn> getTableColumns(final String catalog, final String tableName, final DatabaseMetaData dbMetaData) throws SQLException {
    final ResultSet nonPKcolumnsRS = dbMetaData.getColumns(catalog, null, tableName, null);
    final List<DbTableColumn> columns = new ArrayList<>();

    final ResultSet pkColumnsRS = dbMetaData.getPrimaryKeys(catalog, null, tableName);
    final Set<String> pkColumns = new HashSet<>();

    while (pkColumnsRS.next()) {
      final String colName = pkColumnsRS.getString("COLUMN_NAME");
      pkColumns.add(colName);
    }

    while (nonPKcolumnsRS.next()) {
      final String colName = nonPKcolumnsRS.getString("COLUMN_NAME");
      final int sqlType = nonPKcolumnsRS.getInt("DATA_TYPE");
      boolean isNullable = !pkColumns.contains(colName);
      //nonPKcolumnsRS.getInt("NULLABLE") == DatabaseMetaData.attributeNullable;
      //sqlite reports in this case for PK as true allows nullable
      if (isNullable)
        isNullable = Objects.equals("YES", nonPKcolumnsRS.getString("IS_NULLABLE"));

      columns.add(new DbTableColumn(colName, pkColumns.contains(colName), isNullable, sqlType));
    }
    return columns;
  }
}