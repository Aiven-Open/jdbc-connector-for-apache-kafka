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

package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.DbDialect;

import java.util.List;

/**
 * Builds an UPSERT sql statement.
 */
public class UpsertQueryBuilder implements QueryBuilder {
  private final DbDialect dbDialect;

  public UpsertQueryBuilder(DbDialect dialect) {
    this.dbDialect = dialect;
  }

  /**
   * Builds the sql statement for an upsert
   *
   * @param table         - The target table
   * @param nonKeyColumns - A list of columns in the target table which are not part of the primary key
   * @param keyColumns    - A list of columns in the target table which make the primary key
   * @return An upsert for the dialect
   */
  @Override
  public String build(String table, List<String> nonKeyColumns, List<String> keyColumns) {
    return dbDialect.getUpsertQuery(table, nonKeyColumns, keyColumns);
  }


  public DbDialect getDbDialect() {
    return dbDialect;
  }
}
