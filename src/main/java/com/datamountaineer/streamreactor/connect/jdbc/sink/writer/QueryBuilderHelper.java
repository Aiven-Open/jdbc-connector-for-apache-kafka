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

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.InsertModeEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.DbDialect;

/**
 * Helper class for creating an instance of QueryBuilder
 */
public class QueryBuilderHelper {
  /**
   * Creates an instance of DbDialect from the jdbc sink settings.
   *
   * @param settings - The jdbc sink settings
   * @return - An instance of DbDialect
   */
  public static QueryBuilder from(final JdbcSinkSettings settings) {
    if (settings.getInsertMode() == InsertModeEnum.UPSERT) {
      final DbDialect dialect = DbDialect.fromConnectionString(settings.getConnection());
      return new UpsertQueryBuilder(dialect);
    }
    return new InsertQueryBuilder();
  }
}
