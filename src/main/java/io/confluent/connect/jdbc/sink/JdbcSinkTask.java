package io.confluent.connect.jdbc.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import io.confluent.connect.jdbc.sink.common.DatabaseMetadata;
import io.confluent.connect.jdbc.sink.common.DatabaseMetadataProvider;
import io.confluent.connect.jdbc.sink.config.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.config.JdbcSinkSettings;
import io.confluent.connect.jdbc.sink.writer.JdbcDbWriter;

public class JdbcSinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(JdbcSinkTask.class);

  private JdbcDbWriter writer = null;

  int maxRetries;
  int retryBackoffMs;
  int remainingRetries;

  @Override
  public void start(final Map<String, String> props) {
    final JdbcSinkConfig sinkConfig = new JdbcSinkConfig(props);

    maxRetries = sinkConfig.getInt(JdbcSinkConfig.MAX_RETRIES);
    retryBackoffMs = sinkConfig.getInt(JdbcSinkConfig.RETRY_BACKOFF_MS);
    remainingRetries = maxRetries;

    final JdbcSinkSettings settings = JdbcSinkSettings.from(sinkConfig);
    final DatabaseMetadataProvider provider = new DatabaseMetadataProvider() {
      @Override
      public DatabaseMetadata get(ConnectionProvider connectionProvider) throws SQLException {
        logger.info("Getting metadata for tables: {}", settings.getTableNames());
        return DatabaseMetadata.getDatabaseMetadata(connectionProvider, settings.getTableNames());
      }
    };

    try {
      writer = JdbcDbWriter.from(settings, provider);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      logger.info("Empty list of records received.");
    } else {
      assert (writer != null) : "Writer is not set!";
      final SinkRecord first = records.iterator().next();
      int recordsCount = records.size();
      logger.info("Received {} records. First entry topic:{}  partition:{} offset:{}. Writing them to the database...",
                  recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
      try {
        writer.write(records);
        remainingRetries = maxRetries;
      } catch (SQLException sqle) {
        logger.warn("put failed, remainingRetries={}", remainingRetries, sqle);
        if (remainingRetries == 0) {
          throw new ConnectException(sqle);
        } else {
          remainingRetries--;
          context.timeout(retryBackoffMs);
          throw new RetriableException(sqle);
        }
      }
      logger.info("Finished writing %d records to the database.", recordsCount);
    }
  }

  @Override
  public void stop() {
    logger.info("Stopping Jdbc sink.");
    writer.close();
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

}
