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

public class JdbcSinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(JdbcSinkTask.class);

  private JdbcSinkConfig config;
  private JdbcDbWriter writer;
  private int remainingRetries;

  @Override
  public void start(final Map<String, String> props) {
    logger.info("Starting task");
    config = new JdbcSinkConfig(props);
    writer = new JdbcDbWriter(config);
    remainingRetries = config.maxRetries;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      logger.debug("Empty collection of records received");
    } else {
      final SinkRecord first = records.iterator().next();
      final int recordsCount = records.size();
      logger.debug("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...",
                   recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
      try {
        writer.write(records);
      } catch (SQLException sqle) {
        logger.warn("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries, sqle);
        if (remainingRetries == 0) {
          throw new ConnectException(sqle);
        } else {
          writer.closeQuietly();
          writer = new JdbcDbWriter(config);
          remainingRetries--;
          context.timeout(config.retryBackoffMs);
          throw new RetriableException(sqle);
        }
      }
      remainingRetries = config.maxRetries;
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    logger.info("Stopping task");
    writer.closeQuietly();
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

}
