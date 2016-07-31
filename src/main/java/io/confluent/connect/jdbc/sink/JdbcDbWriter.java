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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;

  Connection connection;

  JdbcDbWriter(final JdbcSinkConfig config, DbDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    initConnection();

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
    }
    connection.commit();
  }

  void initConnection() throws SQLException {
    if (connection == null) {
      connection = newConnection();
    } else if (!connection.isValid(3000)) {
      log.info("The database connection is invalid. Reconnecting...");
      closeQuietly();
      connection = newConnection();
    }
    connection.setAutoCommit(false);
  }

  Connection newConnection() throws SQLException {
    return DriverManager.getConnection(config.connectionUrl, config.connectionUser, config.connectionPassword);
  }

  void closeQuietly() {
    try {
      connection.close();
    } catch (SQLException sqle) {
      log.warn("Ignoring error closing connection", sqle);
    }
  }

  String destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format("Destination table name for topic '%s' is empty using the format string '%s'", topic, config.tableNameFormat));
    }
    return tableName;
  }
}
