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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class TableQuerier implements Comparable<TableQuerier> {
  public enum QueryMode {
    TABLE, // Copying whole tables, with queries constructed automatically
    QUERY // User-specified query
  }

  protected final QueryMode mode;
  protected final String schemaPattern;
  protected final String name;
  protected final String query;
  protected final String topicPrefix;

  // Mutable state
  protected long lastUpdate;
  protected PreparedStatement stmt;
  protected ResultSet resultSet;
  protected Schema schema;

  public TableQuerier(QueryMode mode, String nameOrQuery, String topicPrefix, String schemaPattern) {
    this.mode = mode;
    this.schemaPattern = schemaPattern;
    this.name = mode.equals(QueryMode.TABLE) ? nameOrQuery : null;
    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
    this.topicPrefix = topicPrefix;
    this.lastUpdate = 0;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    if (stmt != null) {
      return stmt;
    }
    createPreparedStatement(db);
    return stmt;
  }

  protected abstract void createPreparedStatement(Connection db) throws SQLException;

  public boolean querying() {
    return resultSet != null;
  }

  public void maybeStartQuery(Connection db) throws SQLException {
    if (resultSet == null) {
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      schema = DataConverter.convertSchema(name, resultSet.getMetaData());
    }
  }

  protected abstract ResultSet executeQuery() throws SQLException;

  public boolean next() throws SQLException {
    return resultSet.next();
  }

  public abstract SourceRecord extractRecord() throws SQLException;

  public void reset(long now) {
    closeResultSetQuietly();
    closeStatementQuietly();
    // TODO: Can we cache this and quickly check that it's identical for the next query
    // instead of constructing from scratch since it's almost always the same
    schema = null;
    lastUpdate = now;
  }

  private void closeStatementQuietly() {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException ignored) {
      }
    }
    stmt = null;
  }

  private void closeResultSetQuietly() {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException ignored) {
      }
    }
    resultSet = null;
  }

  @Override
  public int compareTo(TableQuerier other) {
    if (this.lastUpdate < other.lastUpdate) {
      return -1;
    } else if (this.lastUpdate > other.lastUpdate) {
      return 1;
    } else {
      return this.name.compareTo(other.name);
    }
  }
}
