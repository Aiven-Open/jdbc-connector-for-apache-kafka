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
import java.util.Properties;

import io.confluent.common.config.ConfigException;
import io.confluent.copycat.connector.Task;
import io.confluent.copycat.errors.CopycatException;
import io.confluent.copycat.errors.CopycatRuntimeException;
import io.confluent.copycat.source.SourceConnector;
import io.confluent.copycat.util.ConnectorUtils;
import io.confluent.copycat.util.StringUtils;

/**
 * JdbcConnector is a Copycat Connector implementation that watches a JDBC database and generates
 * Copycat tasks to ingest database contents.
 */
public class JdbcSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);

  Properties configProperties;
  JdbcSourceConnectorConfig config;
  Connection db;

  @Override
  public void start(Properties properties) throws CopycatException {
    try {
      configProperties = properties;
      config = new JdbcSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new CopycatRuntimeException("Couldn't start JdbcSourceConnector due to configuration "
                                        + "error", e);
    }

    String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    log.debug("Trying to connect to {}", dbUrl);
    try {
      db = DriverManager.getConnection(dbUrl);
    } catch (SQLException e) {
      log.error("Couldn't open connection to {}: {}", dbUrl, e);
      throw new CopycatRuntimeException(e);
    }
  }

  @Override
  public Class<? extends Task> getTaskClass() {
    return JdbcSourceTask.class;
  }

  @Override
  public List<Properties> getTaskConfigs(int maxTasks) {
    try {
      // TODO: getTables should run periodically in the background and request reconfiguration if
      // it changes
      List<String> tables = JdbcUtils.getTables(db);
      int numGroups = Math.min(tables.size(), maxTasks);
      List<List<String>> tablesGrouped = ConnectorUtils.groupPartitions(tables, numGroups);
      List<Properties> taskConfigs = new ArrayList<Properties>(tablesGrouped.size());
      for (List<String> taskTables : tablesGrouped) {
        Properties taskProps = new Properties();
        taskProps.putAll(configProperties);
        taskProps.setProperty(JdbcSourceTaskConfig.TABLES_CONFIG,
                              StringUtils.join(taskTables, ","));
        taskConfigs.add(taskProps);
      }
      return taskConfigs;
    } catch (SQLException e) {
      throw new CopycatRuntimeException();
    }
  }

  @Override
  public void stop() throws CopycatException {
    log.debug("Trying to close database connection");
    try {
      db.close();
    } catch (SQLException e) {
      log.error("Failed to close database connection: ", e);
    }
  }
}
