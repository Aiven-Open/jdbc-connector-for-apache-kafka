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

import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;

import java.util.Collection;
import java.util.Map;

/**
 * Contains a list of PreparedStatements to execute as well as the tables affected and the columns referenced.
 */
public class PreparedStatementContext {
  private final Collection<PreparedStatementData> preparedStatements;
  private final Map<String, Collection<SinkRecordField>> tablesToColumnsMap;

  public PreparedStatementContext(Collection<PreparedStatementData> preparedStatements,
                                  Map<String, Collection<SinkRecordField>> tablesToColumnsMap) {
    this.preparedStatements = preparedStatements;
    this.tablesToColumnsMap = tablesToColumnsMap;
  }

  /**
   * Returns the list of PreparedStatements to execute
   *
   * @return Returns the list of PreparedStatements to execute
   */
  public Collection<PreparedStatementData> getPreparedStatements() {
    return preparedStatements;
  }

  /**
   * Returns a map of table name to fields/columns involved
   *
   * @return Returns a map of table name to fields/columns involved
   */
  public Map<String, Collection<SinkRecordField>> getTablesToColumnsMap() {
    return tablesToColumnsMap;
  }
}
