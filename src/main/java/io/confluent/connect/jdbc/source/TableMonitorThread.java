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

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Thread that monitors the database for changes to the set of tables in the database that this
 * connector should load data from.
 */
public class TableMonitorThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(TableMonitorThread.class);

  private final DatabaseDialect dialect;
  private final ConnectionProvider connectionProvider;
  private final ConnectorContext context;
  private final CountDownLatch shutdownLatch;
  private final long pollMs;
  private Set<String> whitelist;
  private Set<String> blacklist;
  private List<TableId> tables;
  private final String configErrorMsg;

  public TableMonitorThread(DatabaseDialect dialect,
      ConnectionProvider connectionProvider,
      ConnectorContext context,
      long pollMs,
      Set<String> whitelist,
      Set<String> blacklist
  ) {
    this.dialect = dialect;
    this.connectionProvider = connectionProvider;
    this.context = context;
    this.shutdownLatch = new CountDownLatch(1);
    this.pollMs = pollMs;
    this.whitelist = whitelist;
    this.blacklist = blacklist;
    this.tables = null;

    if (whitelist != null) {
      configErrorMsg = "'" + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + "'";
    } else if (blacklist != null) {
      configErrorMsg = "'" + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + "'";
    } else {
      configErrorMsg = "'" + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + "' or '"
          + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + "'";
    }
  }

  @Override
  public void run() {
    while (shutdownLatch.getCount() > 0) {
      try {
        if (updateTables()) {
          context.requestTaskReconfiguration();
        }
      } catch (Exception e) {
        context.raiseError(e);
        throw e;
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

  public synchronized List<TableId> tables() {
    //TODO: Timeout should probably be user-configurable or class-level constant
    final long timeout = 10000L;
    long started = System.currentTimeMillis();
    long now = started;
    while (tables == null && now - started < timeout) {
      try {
        wait(timeout - (now - started));
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

  public void shutdown() {
    shutdownLatch.countDown();
  }

  private synchronized boolean updateTables() {
    final List<TableId> tables;
    try {
      tables = dialect.tableIds(connectionProvider.getConnection());
      log.debug("Got the following tables: " + Arrays.toString(tables.toArray()));
    } catch (SQLException e) {
      log.error(
          "Error while trying to get updated table list, ignoring and waiting for next table poll"
          + " interval",
          e
      );
      connectionProvider.close();
      return false;
    }

    final List<TableId> filteredTables = new ArrayList<>(tables.size());
    if (whitelist != null) {
      for (TableId table : tables) {
        String fqn1 = dialect.expressionBuilder().append(table, false).toString();
        String fqn2 = dialect.expressionBuilder().append(table, true).toString();
        if (whitelist.contains(fqn1) || whitelist.contains(fqn2)
            || whitelist.contains(table.tableName())) {
          filteredTables.add(table);
        }
      }
    } else if (blacklist != null) {
      for (TableId table : tables) {
        String fqn1 = dialect.expressionBuilder().append(table, false).toString();
        String fqn2 = dialect.expressionBuilder().append(table, true).toString();
        if (!(blacklist.contains(fqn1) || blacklist.contains(fqn2)
              || blacklist.contains(table.tableName()))) {
          filteredTables.add(table);
        }
      }
    } else {
      filteredTables.addAll(tables);
    }

    Map<String, List<TableId>> duplicates = filteredTables.stream()
        .collect(Collectors.groupingBy(TableId::tableName))
        .entrySet().stream()
        .filter(entry -> entry.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (!duplicates.isEmpty()) {
      log.info("The filtered tables are {} ", filteredTables);
      log.error("Connector uses unqualified table name as the topic name and has detected "
          + "duplicate unqualified table names. This could lead to mixed data type and "
          + "subsequently errors during the processing. Hence to prevent processing errors, "
          + "JDBC Source connector fails fast fr such configurations. To be able to start the "
          + "connector, update connector's {} config to include exactly one table in each of the "
          + "below listed tables.\n {} ", configErrorMsg, duplicates);
      // this will force the connector to fail configuration as it doesn't have any table to process
      filteredTables.clear();
    }

    if (!filteredTables.equals(this.tables)) {
      log.debug(
          "After filtering the tables are: {}",
          dialect.expressionBuilder()
                 .appendList()
                 .delimitedBy(",")
                 .of(filteredTables)
      );
      List<TableId> previousTables = this.tables;
      this.tables = filteredTables;
      notifyAll();
      // Only return true if the table list wasn't previously null, i.e. if this was not the
      // first table lookup
      return previousTables != null;
    }

    return false;
  }
}
