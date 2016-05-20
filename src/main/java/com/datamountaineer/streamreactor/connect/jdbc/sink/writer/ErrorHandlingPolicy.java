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

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.ErrorPolicyEnum;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * Defines the approach to take when the db operation fails inserting the new records
 */
public interface ErrorHandlingPolicy {
  /**
   * Called when the sql operation to insert the SinkRecords fails
   *  @param records    The list of records received by the SinkTask
   * @param error      - The error raised when the insert failed
   */
  void handle(Collection<SinkRecord> records, final Throwable error, final int retryCount);
}

final class ErrorHandlingPolicyHelper {
  /**
   * Creates a new instance of ErrorHandling policy given the enum value.
   * @param value - The enum value specifying how errors should be handled during a database data insert.
   * @return new instance of ErrorHandlingPolicy
   */
  public static ErrorHandlingPolicy from(final ErrorPolicyEnum value) {
    switch (value) {
      case NOOP:
        return new NoopErrorHandlingPolicy();

      case THROW:
        return new ThrowErrorHandlingPolicy();

      case RETRY:
        return new RetryErrorHandlingPolicy();

      default:
        throw new RuntimeException(value + " error handling policy is not recognized.");
    }

  }
}


