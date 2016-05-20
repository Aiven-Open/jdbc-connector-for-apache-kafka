/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

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
