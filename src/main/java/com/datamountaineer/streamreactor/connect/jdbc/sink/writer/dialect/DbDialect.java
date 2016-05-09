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

package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import java.util.List;

/**
 * Describes which SQL dialect to use. Different databases support different syntax for upserts.
 */
public abstract class DbDialect {
  /**
   * Gets the query allowing to insert a new row into the RDBMS even if it does previously exists
   *
   * @param table The table to build the insert for
   * @param columns The list of columns to update
   * @return The key columns
   */
  public abstract String getUpsertQuery(final String table,
                                        final List<String> columns,
                                        final List<String> keyColumns);


  /**
   * Maps a SQL dialect type to an instance of a derived class of DbDialect
   * @param type - The sql dialect type value
   * @return - An instance of DbDialect
   */
  public static DbDialect fromDialectType(DbDialectTypeEnum type) {
    switch (type) {
      case MSSQL:
      case ORACLE:
        return new Sql2003Dialect();

      case SQLITE:
        return new SQLiteDialect();

      case MYSQL:
        return new MySqlDialect();

      default:
        throw new IllegalArgumentException(type + " is not handled");
    }
  }
}