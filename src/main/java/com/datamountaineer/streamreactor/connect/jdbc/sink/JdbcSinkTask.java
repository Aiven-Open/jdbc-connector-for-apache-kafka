package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.ConnectionProvider;
import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadataProvider;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.ErrorPolicyEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.JdbcDbWriter;
import com.google.common.base.Joiner;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class JdbcSinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(JdbcSinkTask.class);

  private JdbcDbWriter writer = null;

  @Override
  public void start(final Map<String, String> props) {
    final JdbcSinkConfig sinkConfig = new JdbcSinkConfig(props);
    int retryInterval = sinkConfig.getInt(JdbcSinkConfig.RETRY_INTERVAL);

    final JdbcSinkSettings settings = JdbcSinkSettings.from(sinkConfig);

    if (settings.getErrorPolicy().equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(retryInterval);
    }

    DatabaseMetadataProvider provider = new DatabaseMetadataProvider() {
      @Override
      public DatabaseMetadata get(ConnectionProvider connectionProvider) throws SQLException {
        logger.info("Getting tables metadata for " + Joiner.on(",").join(settings.getTableNames()));
        return DatabaseMetadata.getDatabaseMetadata(connectionProvider, settings.getTableNames());
      }
    };

    try {
      writer = JdbcDbWriter.from(settings, provider);
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
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
      writer.write(records);
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
