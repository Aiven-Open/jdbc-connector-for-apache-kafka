/*
 * Copyright 2016 Confluent Inc.
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
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;

public class JdbcDbWriter {

  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DbDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = new CachedConnectionProvider(config.connectionUrl, config.connectionUser, config.connectionPassword) {
      @Override
      protected void onConnect(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getValidConnection();

    final Map<String, BufferedRecords> bufferByTable = new HashMap<>();
    for (SinkRecord record : records) {
      final String table = destinationTable(record.topic());
      BufferedRecords buffer = bufferByTable.get(table);
      if (buffer == null) {
        buffer = new BufferedRecords(config, table, dbDialect, dbStructure, connection);
        bufferByTable.put(table, buffer);
      }
      buffer.add(record);
    }
    for (BufferedRecords buffer : bufferByTable.values()) {
      buffer.flush();
      buffer.close();
    }
    connection.commit();
  }

  void closeQuietly() {
    cachedConnectionProvider.closeQuietly();
  }

  String destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format("Destination table name for topic '%s' is empty using the format string '%s'", topic, config.tableNameFormat));
    }
    return tableName;
  }
}
