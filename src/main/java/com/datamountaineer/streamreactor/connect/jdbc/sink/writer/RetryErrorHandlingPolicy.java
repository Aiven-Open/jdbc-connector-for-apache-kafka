package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.google.common.collect.Iterators;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * The policy propagates the error further
 */
public final class RetryErrorHandlingPolicy implements ErrorHandlingPolicy {
  private static final Logger logger = LoggerFactory.getLogger(RetryErrorHandlingPolicy.class);

  @Override
  public void handle(Collection<SinkRecord> records, final Throwable error, final int retryCount) {
    if (retryCount == 0) {
      throw new RuntimeException(error);
    } else {
      logger.warn(String.format("Error policy set to RETRY. The following events will be replayed. " +
          "Remaining attempts %d", retryCount));

      if (!records.isEmpty()) {
        final SinkRecord firstRecord = Iterators.getNext(records.iterator(), null);
        assert firstRecord != null;
        logger.warn(String.format("Going to retry inserting data starting at topic: %s offset: %d partition: %d",
            firstRecord.topic(),
            firstRecord.kafkaOffset(),
            firstRecord.kafkaPartition()));
        throw new RetriableException(error);
      } else {
        throw new RetriableException(error);
      }
    }
  }
}
