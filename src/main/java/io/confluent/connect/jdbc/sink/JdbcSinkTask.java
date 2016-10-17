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

import io.confluent.connect.jdbc.sink.dialect.DbDialect;

public class JdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

  JdbcSinkConfig config;
  JdbcDbWriter writer;
  int remainingRetries;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting task");
    config = new JdbcSinkConfig(props);
    initWriter();
    remainingRetries = config.maxRetries;
  }

  void initWriter() {
    final DbDialect dbDialect = DbDialect.fromConnectionString(config.connectionUrl);
    final DbStructure dbStructure = new DbStructure(dbDialect);
    log.info("Initializing writer using SQL dialect: {}", dbDialect.getClass().getSimpleName());
    writer = new JdbcDbWriter(config, dbDialect, dbStructure);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...",
              recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
    try {
      writer.write(records);
    } catch (SQLException sqle) {
      log.warn("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries, sqle);
      if (remainingRetries == 0) {
        throw new ConnectException(sqle);
      } else {
        writer.closeQuietly();
        initWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        throw new RetriableException(sqle);
      }
    }
    remainingRetries = config.maxRetries;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    log.info("Stopping task");
    writer.closeQuietly();
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

}
