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
  private static final Logger logger = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final Map<String, JdbcSinkConfig> contextualConfigCache = new HashMap<>();

  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;

  Connection connection;

  JdbcDbWriter(final JdbcSinkConfig config) {
    this.config = config;
    dbDialect = DbDialect.fromConnectionString(config.connectionUrl);
    dbStructure = new DbStructure(dbDialect);
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    initConnection();

    final Map<String, BufferedRecords> bufferByTable = new HashMap<>();
    for (SinkRecord record : records) {
      final String table = destinationTable(record.topic());
      BufferedRecords buffer = bufferByTable.get(table);
      if (buffer == null) {
        buffer = new BufferedRecords(cachedContextualConfig(table), table, dbDialect, dbStructure, connection);
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
      logger.info("The database connection is invalid. Reconnecting...");
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
      logger.warn("Ignoring error closing connection", sqle);
    }
  }

  JdbcSinkConfig cachedContextualConfig(String context) {
    JdbcSinkConfig contextualConfig = contextualConfigCache.get(context);
    if (contextualConfig == null) {
      contextualConfig = config.contextualConfig(context);
      contextualConfigCache.put(context, contextualConfig);
    }
    return contextualConfig;
  }

  String destinationTable(String topic) {
    final String tableNameFormat = cachedContextualConfig(topic).tableNameFormat.trim();
    final String tableName = String.format(tableNameFormat, topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format("Destination table name for topic '%s' is empty using the format string '%s'", topic, tableNameFormat));
    }
    return tableName;
  }
}
