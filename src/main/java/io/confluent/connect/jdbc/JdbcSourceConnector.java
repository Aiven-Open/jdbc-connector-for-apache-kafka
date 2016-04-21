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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.jdbc.util.StringUtils;
import io.confluent.connect.jdbc.util.Version;

/**
 * JdbcConnector is a Kafka Connect Connector implementation that watches a JDBC database and
 * generates tasks to ingest database contents.
 */
public class JdbcSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);

  private static final long MAX_TIMEOUT = 10000L;

  private Map<String, String> configProperties;
  private JdbcSourceConnectorConfig config;
  private Connection db;
  private TableMonitorThread tableMonitorThread;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) throws ConnectException {
    try {
      configProperties = properties;
      config = new JdbcSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration "
                                 + "error", e);
    }

    String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    log.debug("Trying to connect to {}", dbUrl);
    try {
      db = DriverManager.getConnection(dbUrl);
    } catch (SQLException e) {
      log.error("Couldn't open connection to {}: {}", dbUrl, e);
      throw new ConnectException(e);
    }

    long tablePollMs = config.getLong(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG);
    List<String> whitelist = config.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
    List<String> blacklist = config.getList(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);
    if (whitelistSet != null && blacklistSet != null)
      throw new ConnectException(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
                                 + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " are "
                                 + "exclusive.");
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    if (!query.isEmpty()) {
      if (whitelistSet != null || blacklistSet != null)
        throw new ConnectException(JdbcSourceConnectorConfig.QUERY_CONFIG + " may not be combined"
                                   + " with whole-table copying settings.");
      // Force filtering out the entire set of tables since the one task we'll generate is for the
      // query.
      whitelistSet = Collections.emptySet();
    }
    tableMonitorThread = new TableMonitorThread(db, context, tablePollMs, whitelistSet,
                                                blacklistSet);
    tableMonitorThread.start();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JdbcSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    if (!query.isEmpty()) {
      List<Map<String, String>> taskConfigs = new ArrayList<>(1);
      Map<String, String> taskProps = new HashMap<>(configProperties);
      taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
      taskConfigs.add(taskProps);
      return taskConfigs;
    } else {
      List<String> currentTables = tableMonitorThread.tables();
      int numGroups = Math.min(currentTables.size(), maxTasks);
      List<List<String>> tablesGrouped = ConnectorUtils.groupPartitions(currentTables, numGroups);
      List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
      for (List<String> taskTables : tablesGrouped) {
        Map<String, String> taskProps = new HashMap<>(configProperties);
        taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG,
                      StringUtils.join(taskTables, ","));
        taskConfigs.add(taskProps);
      }
      return taskConfigs;
    }
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping table monitoring thread");
    tableMonitorThread.shutdown();
    try {
      tableMonitorThread.join(MAX_TIMEOUT);
    } catch (InterruptedException e) {
      // Ignore, shouldn't be interrupted
    }

    log.debug("Trying to close database connection");
    try {
      db.close();
    } catch (SQLException e) {
      log.error("Failed to close database connection: ", e);
    }
  }

  @Override
  public ConfigDef config() {
    return JdbcSourceConnectorConfig.config;
  }
}
