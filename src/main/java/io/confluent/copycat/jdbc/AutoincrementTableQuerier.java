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
 * AutoincrementTableQuerier performs incremental loading of data by using an autoincrementing
 * column to identify new data.
 */
public class AutoincrementTableQuerier extends TableQuerier {
  private String autoincrementColumn;
  private Integer maxOffset = null;

  public AutoincrementTableQuerier(String name, Integer offset) {
    super(name);
    maxOffset = offset;
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT * FROM \"");
    builder.append(name);
    builder.append("\"");
    autoincrementColumn = JdbcUtils.getAutoincrementColumn(db, name);
    builder.append(" WHERE \"");
    builder.append(autoincrementColumn);
    builder.append("\" > ?");
    builder.append(" ORDER BY \"");
    builder.append(autoincrementColumn);
    builder.append("\" ASC");

    stmt = db.prepareStatement(builder.toString());
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    stmt.setInt(1, (maxOffset == null ? -1 : maxOffset));
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    GenericRecord record = DataConverter.convertRecord(schema, resultSet);
    // stream offset and key are both the autoincrement value, which is the incrementing unique
    // ID for the row
    int id = (Integer)record.get(autoincrementColumn);
    // Any
    assert maxOffset == null || id > maxOffset;
    maxOffset = id;
    return new SourceRecord(name, id, name, null, id, record);
  }
}
