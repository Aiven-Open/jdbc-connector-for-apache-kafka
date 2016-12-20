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

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.util.JdbcUtils;

/**
 * BulkTableQuerier always returns the entire table.
 */
public class BulkTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);

  public BulkTableQuerier(QueryMode mode, String name, String schemaPattern,
                          String topicPrefix, boolean mapNumerics) {
    super(mode, name, topicPrefix, schemaPattern, mapNumerics);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    switch (mode) {
      case TABLE:
        String quoteString = JdbcUtils.getIdentifierQuoteString(db);
        String queryString = "SELECT * FROM " + JdbcUtils.quoteString(name, quoteString);
        log.debug("{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
        break;
      case QUERY:
        log.debug("{} prepared SQL query: {}", this, query);
        stmt = db.prepareStatement(query);
        break;
    }
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = DataConverter.convertRecord(schema, resultSet, mapNumerics);
    // TODO: key from primary key? partition?
    final String topic;
    final Map<String, String> partition;
    switch (mode) {
      case TABLE:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
        topic = topicPrefix + name;
        break;
      case QUERY:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                             JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
        topic = topicPrefix;
        break;
      default:
        throw new ConnectException("Unexpected query mode: " + mode);
    }
    return new SourceRecord(partition, null, topic, record.schema(), record);
  }

  @Override
  public String toString() {
    return "BulkTableQuerier{" +
           "name='" + name + '\'' +
           ", query='" + query + '\'' +
           ", topicPrefix='" + topicPrefix + '\'' +
           '}';
  }

}
