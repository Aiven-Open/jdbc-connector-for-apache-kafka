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


package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;

import java.util.List;

/**
 * Describes which SQL dialect to use. Different databases support different syntax for upserts.
 */
public abstract class DbDialect {
  /**
   * Gets the query allowing to insert a new row into the RDBMS even if it does previously exists
   *
   * @param table       - Contains the name of the target table
   * @param columns     - Contains the table non primary key columns which will get data inserted in
   * @param keyColumns- Contains the table primary key columns
   * @return
   */
  public abstract String getUpsertQuery(final String table,
                                        final List<String> columns,
                                        final List<String> keyColumns);


  /**
   * Maps a JDBC  URI to an instance of a derived class of DbDialect
   *
   * @param connection - The jdbc connection uri
   * @return - An instance of DbDialect
   */
  public static DbDialect fromConnectionString(final String connection) {
    ParameterValidator.notNullOrEmpty(connection, "connection");
    if (!connection.startsWith("jdbc:")) {
      throw new IllegalArgumentException("connection is not valid. Expecting a jdbc uri: jdbc:protocol//server:port/...");
    }

//sqlite URIs are not in the format jdbc:protocol://FILE but jdbc:protocol:file
    if (connection.startsWith("jdbc:sqlite:")) {
      return new SQLiteDialect();
    }

    if (connection.startsWith("jdbc:oracle:thin:@")) {
      return new Sql2003Dialect();
    }

    final String protocol = extractProtocol(connection).toLowerCase();
    switch (protocol) {
      case "microsoft:sqlserver":
        return new SqlServerDialect();

      case "mysql":
        return new MySqlDialect();

      case "postgresql":
        return new PostgreDialect();

      default:
        throw new IllegalArgumentException(String.format("%s jdbc is not handled.", protocol));
    }
  }

  /**
   * Extracts the database protocol from a jdbc URI.
   *
   * @param connection
   * @return
   */
  public static String extractProtocol(final String connection) {
    ParameterValidator.notNullOrEmpty(connection, "connection");
    if (!connection.startsWith("jdbc:"))
      throw new IllegalArgumentException("connection is not a valid jdbc URI");

    int index = connection.indexOf("://", "jdbc:".length());
    if (index < 0) {
      throw new IllegalArgumentException(String.format("%s is not a valid jdbc uri.", connection));
    }
    return connection.substring("jdbc:".length(), index);
  }
}