package io.confluent.connect.jdbc.sink.writer;

import com.google.common.collect.Iterators;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      if (!records.isEmpty()) {
        final SinkRecord firstRecord = Iterators.getNext(records.iterator(), null);
        assert firstRecord != null;
        logger.warn("Going to retry inserting data starting at topic: {} offset: {} partition: {}. Remaining attempts {}",
                    firstRecord.topic(),
                    firstRecord.kafkaOffset(),
                    firstRecord.kafkaPartition(),
                    retryCount);
        throw new RetriableException(error);
      } else {
        throw new RetriableException(error);
      }
    }
  }
}
