/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.copycat.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utilties for interacting with a JDBC database.
 */
public class JdbcUtils {

  /**
   * The default table types to include when listing tables if none are specified. Valid values
   * are those specified by the @{java.sql.DatabaseMetaData#getTables} method's TABLE_TYPE column.
   * The default only includes standard, user-defined tables.
   */
  public static final Set<String> DEFAULT_TABLE_TYPES = Collections.unmodifiableSet(
      new HashSet<String>(Arrays.asList("TABLE"))
  );

  private static final int GET_TABLES_TYPE_COLUMN = 4;
  private static final int GET_TABLES_NAME_COLUMN = 3;

  private static final int GET_COLUMNS_COLUMN_NAME = 4;
  private static final int GET_COLUMNS_IS_AUTOINCREMENT = 23;

  /**
   * Get a list of tables in the database. This uses the default filters, which only include
   * user-defined tables.
   * @param conn database connection
   * @return a list of tables
   * @throws SQLException
   */
  public static List<String> getTables(Connection conn) throws SQLException {
    return getTables(conn, DEFAULT_TABLE_TYPES);
  }

  /**
   * Get a list of table names in the database.
   * @param conn database connection
   * @param types a set of table types that should be included in the results
   * @throws SQLException
   */
  public static List<String> getTables(Connection conn, Set<String> types) throws SQLException {
    DatabaseMetaData metadata = conn.getMetaData();
    ResultSet rs = metadata.getTables(null, null, "%", null);
    List<String> tableNames = new ArrayList<String>();
    while (rs.next()) {
      if (types.contains(rs.getString(GET_TABLES_TYPE_COLUMN))) {
        tableNames.add(rs.getString(GET_TABLES_NAME_COLUMN));
      }
    }
    return tableNames;
  }

  /**
   * Look up the autoincrement column for the specified table.
   * @param conn database connection
   * @param table the table to
   * @return the name of the column that is an autoincrement column, or null if there is no
   *         autoincrement column or more than one exists
   * @throws SQLException
   */
  public static String getAutoincrementColumn(Connection conn, String table) throws SQLException {
    String result = null;
    ResultSet rs = conn.getMetaData().getColumns(null, null, table, "%");
    while(rs.next()) {
      if (rs.getString(GET_COLUMNS_IS_AUTOINCREMENT).equals("YES")) {
        if (result != null) {
          // More than one
          return null;
        }
        result = rs.getString(GET_COLUMNS_COLUMN_NAME);
      }
    }
    return result;
  }
}
