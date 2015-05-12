/**
 * Copyright 2015 Confluent Inc.
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

package io.confluent.copycat.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.source.SourceRecord;

/**
 * BulkTableQuerier always returns the entire table.
 */
public class BulkTableQuerier extends TableQuerier {

  public BulkTableQuerier(String name) {
    super(name);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    stmt = db.prepareStatement("SELECT * FROM \"" + name + "\"");
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    GenericRecord record = DataConverter.convertRecord(schema, resultSet);
    // TODO: key from primary key? partition?
    return new SourceRecord(name, null, name, null, null, record);
  }

}
