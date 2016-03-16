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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
  private Set<String> whitelist;
  private Set<String> blacklist;
  private List<String> tables;

  public TableMonitorThread(Connection db, ConnectorContext context, long pollMs,
                            Set<String> whitelist, Set<String> blacklist) {
    this.db = db;
    this.context = context;
    this.shutdownLatch = new CountDownLatch(1);
    this.pollMs = pollMs;
    this.whitelist = whitelist;
    this.blacklist = blacklist;
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
    //TODO: Timeout should probably be user-configurable or class-level constant
    final long timeout = 10000L;
    synchronized (db) {
      long started = System.currentTimeMillis();
      long now = started;
      while (tables == null && now - started < timeout) {
        try {
          db.wait(timeout - (now - started));
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
        log.debug("Got the following tables: " + Arrays.toString(tables.toArray()));
      } catch (SQLException e) {
        log.error("Error while trying to get updated table list, ignoring and waiting for next "
                  + "table poll interval", e);
        return false;
      }

      final List<String> filteredTables;
      if (whitelist != null) {
        filteredTables = new ArrayList<>(tables.size());
        for (String table : tables) {
          if (whitelist.contains(table)) {
            filteredTables.add(table);
          }
        }
      } else if (blacklist != null) {
        filteredTables = new ArrayList<>(tables.size());
        for (String table : tables) {
          if (!blacklist.contains(table)) {
            filteredTables.add(table);
          }
        }
      } else {
        filteredTables = tables;
      }

      if (!filteredTables.equals(this.tables)) {
        log.debug("After filtering we got tables: " + Arrays.toString(filteredTables.toArray()));
        List<String> previousTables = this.tables;
        this.tables = filteredTables;
        db.notifyAll();
        // Only return true if the table list wasn't previously null, i.e. if this was not the
        // first table lookup
        return previousTables != null;
      }

      return false;
    }
  }
}
