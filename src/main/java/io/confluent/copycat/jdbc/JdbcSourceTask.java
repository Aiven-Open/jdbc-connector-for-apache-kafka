/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.copycat.jdbc;

import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.common.config.ConfigException;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;

/**
 * JdbcSourceTask is a Copycat SourceTask implementation that reads from JDBC databases and
 * generates Copycat records.
 */
public class JdbcSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  static final String INCREASING_FIELD = "increasing";
  static final String TIMESTAMP_FIELD = "timestamp";

  private Time time;
  private JdbcSourceConnectorConfig connectorConfig;
  private JdbcSourceTaskConfig config;
  private Connection db;
  private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();
  private AtomicBoolean stop;

  public JdbcSourceTask() {
    this.time = new SystemTime();
  }

  public JdbcSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public void start(Properties properties) {
    try {
      connectorConfig = new JdbcSourceConnectorConfig(properties);
      config = new JdbcSourceTaskConfig(properties);
    } catch (ConfigException e) {
      throw new CopycatException("Couldn't start JdbcSourceTask due to configuration error",
                                        e);
    }

    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    if (tables.isEmpty()) {
      throw new CopycatException("Invalid configuration: each JdbcSourceTask must have at "
                                        + "least one table assigned to it");
    }

    String mode = connectorConfig.getString(JdbcSourceConnectorConfig.MODE_CONFIG);
    Map<Map<String, String>, Map<String, Object>> offsets = null;
    if (mode.equals(JdbcSourceConnectorConfig.MODE_INCREASING) ||
        mode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP) ||
        mode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREASING)) {
      List<Map<String, String>> partitions = new ArrayList<>(tables.size());
      for (String table : tables) {
        Map<String, String> partition =
            Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, table);
        partitions.add(partition);
      }
      offsets = context.offsetStorageReader().offsets(partitions);
    }

    for (String tableName : tables) {
      String increasingColumn
          = connectorConfig.getString(JdbcSourceConnectorConfig.INCREASING_COLUMN_NAME_CONFIG);
      String timestampColumn
          = connectorConfig.getString(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
      Map<String, String> partition =
          Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, tableName);
      Map<String, Object> offset = offsets == null ? null : offsets.get(partition);
      Long increasingOffset = offset == null ? null :
                              (Long)offset.get(INCREASING_FIELD);
      Long timestampOffset = offset == null ? null :
                             (Long)offset.get(TIMESTAMP_FIELD);

      if (mode.equals(JdbcSourceConnectorConfig.MODE_BULK)) {
        tableQueue.add(new BulkTableQuerier(tableName));
      } else if (mode.equals(JdbcSourceConnectorConfig.MODE_INCREASING)) {
        tableQueue.add(new TimestampIncreasingTableQuerier(
            tableName, null, null, increasingColumn, increasingOffset));
      } else if (mode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP)) {
        tableQueue.add(new TimestampIncreasingTableQuerier(
            tableName, timestampColumn, timestampOffset, null, null));
      } else if (mode.endsWith(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREASING)) {
        tableQueue.add(new TimestampIncreasingTableQuerier(
            tableName, timestampColumn, timestampOffset, increasingColumn, increasingOffset));
      }
    }

    String dbUrl = connectorConfig.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    log.debug("Trying to connect to {}", dbUrl);
    try {
      db = DriverManager.getConnection(dbUrl);
    } catch (SQLException e) {
      log.error("Couldn't open connection to {}: {}", dbUrl, e);
      throw new CopycatException(e);
    }

    stop = new AtomicBoolean(false);
  }

  @Override
  public void stop() throws CopycatException {
    if (stop != null) {
      stop.set(true);
    }
    if (db != null) {
      log.debug("Trying to close database connection");
      try {
        db.close();
      } catch (SQLException e) {
        log.error("Failed to close database connection: ", e);
      }
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long now = time.milliseconds();
    log.trace("{} Polling for new data");
    while (!stop.get()) {
      // If not in the middle of an update, wait for next update time
      TableQuerier querier = tableQueue.peek();
      if (!querier.querying()) {
        long nextUpdate = querier.getLastUpdate() +
                          connectorConfig.getInt(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG);
        long untilNext = nextUpdate - now;
        log.trace("Waiting {} ms to poll {} next", untilNext, querier.getName());
        if (untilNext > 0) {
          time.sleep(untilNext);
          now = time.milliseconds();
          // Handle spurious wakeups
          continue;
        }
      }

      List<SourceRecord> results = new ArrayList<>();
      try {
        log.trace("Checking for next block of results from {}", querier.getName());
        querier.maybeStartQuery(db);

        int batchMaxRows = connectorConfig.getInt(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG);
        boolean hadNext = true;
        while (results.size() < batchMaxRows && (hadNext = querier.next())) {
          results.add(querier.extractRecord());
        }

        // If we finished processing the results from this query, we can clear it out
        if (!hadNext) {
          log.trace("Closing this query for {}", querier.getName());
          TableQuerier removedQuerier = tableQueue.poll();
          assert removedQuerier == querier;
          now = time.milliseconds();
          querier.close(now);
          tableQueue.add(querier);
        }

        if (results.isEmpty()) {
          log.trace("No updates for {}", querier.getName());
          continue;
        }

        log.trace("Returning {} records for {}", results.size(), querier.getName());
        return results;
      } catch (SQLException e) {
        log.error("Failed to run query for table {}: {}", querier.getName(), e);
        return null;
      }
    }

    // Only in case of shutdown
    return null;
  }

}
