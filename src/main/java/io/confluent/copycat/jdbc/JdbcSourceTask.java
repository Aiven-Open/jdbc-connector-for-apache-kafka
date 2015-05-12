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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.common.config.ConfigException;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.copycat.data.Schema;
import io.confluent.copycat.data.SchemaBuilder;
import io.confluent.copycat.errors.CopycatException;
import io.confluent.copycat.errors.CopycatRuntimeException;
import io.confluent.copycat.source.SourceRecord;
import io.confluent.copycat.source.SourceTask;

/**
 * JdbcSourceTask is a Copycat SourceTask implementation that reads from JDBC databases and
 * generates Copycat records.
 */
public class JdbcSourceTask extends SourceTask<Object, Object> {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  private static final Schema offsetSchema = SchemaBuilder.builder().longType();

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
      throw new CopycatRuntimeException("Couldn't start JdbcSourceTask due to configuration error",
                                        e);
    }

    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    if (tables.isEmpty()) {
      throw new CopycatRuntimeException("Invalid configuration: each JdbcSourceTask must have at "
                                        + "least one table assigned to it");
    }

    boolean autoIncrementMode = connectorConfig.getBoolean(
        JdbcSourceConnectorConfig.AUTOINCREMENT_CONFIG);
    Map<Object, Object> offsets = null;
    if (autoIncrementMode) {
      List<Object> streams = new ArrayList<Object>(tables.size());
      for(String table : tables) {
        streams.add(table);
      }
      offsets = context.getOffsetStorageReader().getOffsets(streams, offsetSchema);
    }
    for(String tableName : tables) {
      if (autoIncrementMode) {
        tableQueue.add(new AutoincrementTableQuerier(tableName, (Integer)offsets.get(tableName)));
      } else {
        tableQueue.add(new BulkTableQuerier(tableName));
      }
    }

    String dbUrl = connectorConfig.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    log.debug("Trying to connect to {}", dbUrl);
    try {
      db = DriverManager.getConnection(dbUrl);
    } catch (SQLException e) {
      log.error("Couldn't open connection to {}: {}", dbUrl, e);
      throw new CopycatRuntimeException(e);
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
    while (!stop.get()) {
      // If not in the middle of an update, wait for next update time
      TableQuerier querier = tableQueue.peek();
      if (!querier.querying()) {
        long nextUpdate = querier.getLastUpdate() +
                          connectorConfig.getInt(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG);
        long untilNext = nextUpdate - now;
        if (untilNext > 0) {
          time.sleep(untilNext);
          now = time.milliseconds();
          // Handle spurious wakeups
          continue;
        }
      }

      List<SourceRecord> results = new ArrayList<SourceRecord>();
      try {
        querier.maybeStartQuery(db);

        int batchMaxRows = connectorConfig.getInt(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG);
        boolean hadNext = true;
        while (results.size() < batchMaxRows && (hadNext = querier.next())) {
          results.add(querier.extractRecord());
        }

        // If we finished processing the results from this query, we can clear it out
        if (!hadNext) {
          TableQuerier removedQuerier = tableQueue.poll();
          assert removedQuerier == querier;
          now = time.milliseconds();
          querier.close(now);
          tableQueue.add(querier);
        }

        if (results.isEmpty()) {
          continue;
        }

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
