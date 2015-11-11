/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Thread that monitors the database for changes to the set of tables in the database that this
 * connector should load data from.
 */
public class TableMonitorThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(TableMonitorThread.class);

  private final Connection db;
  private final ConnectorContext context;
  private final CountDownLatch shutdownLatch;
  private final long pollMs;
  private List<String> tables;

  public TableMonitorThread(Connection db, ConnectorContext context, long pollMs) {
    this.db = db;
    this.context = context;
    this.shutdownLatch = new CountDownLatch(1);
    this.pollMs = pollMs;
    this.tables = null;
  }

  @Override
  public void run() {
    while (shutdownLatch.getCount() > 0) {
      if (updateTables()) {
        context.requestTaskReconfiguration();
      }

      try {
        boolean shuttingDown = shutdownLatch.await(pollMs, TimeUnit.MILLISECONDS);
        if (shuttingDown) {
          return;
        }
      } catch (InterruptedException e) {
        log.error("Unexpected InterruptedException, ignoring: ", e);
      }
    }
  }

  public List<String> tables() {
    final long TIMEOUT = 10000L;
    synchronized (db) {
      long started = System.currentTimeMillis();
      long now = started;
      while (tables == null && now - started < TIMEOUT) {
        try {
          db.wait(TIMEOUT - (now - started));
        } catch (InterruptedException e) {
          // Ignore
        }
        now = System.currentTimeMillis();
      }
      if (tables == null) {
        throw new ConnectException("Tables could not be updated quickly enough.");
      }
      return tables;
    }
  }

  public void shutdown() {
    shutdownLatch.countDown();
  }

  // Update tables and return true if the
  private boolean updateTables() {
    synchronized (db) {
      final List<String> tables;
      try {
        tables = JdbcUtils.getTables(db);
      } catch (SQLException e) {
        log.error("Error while trying to get updated table list, ignoring and waiting for next "
                  + "table poll interval", e);
        return false;
      }

      // TODO: Any filtering like whitelists/blacklists or regex matches should be applied here.

      if (!tables.equals(this.tables)) {
        List<String> previousTables = this.tables;
        this.tables = tables;
        db.notifyAll();
        // Only return true if the table list wasn't previously null, i.e. if this was not the
        // first table lookup
        return previousTables != null;
      }

      return false;
    }
  }
}
