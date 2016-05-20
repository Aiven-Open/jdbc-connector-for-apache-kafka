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

import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Binds the SinkRecord entries to the sql PreparedStatement.
 */
final class PreparedStatementBindData {
  /**
   * Binds the values to the given PreparedStatement
   *
   * @param statement - the sql prepared statement to be executed
   * @param binders   -  The SinkRecord values to be bound to the sql statement
   */
  public static void apply(PreparedStatement statement, Iterable<PreparedStatementBinder> binders) throws SQLException {

    int index = 1;
    for (final PreparedStatementBinder binder : binders) {
      binder.bind(index++, statement);
    }
  }
}
