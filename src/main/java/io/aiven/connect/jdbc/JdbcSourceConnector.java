/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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
 */

package io.aiven.connect.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialects;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.aiven.connect.jdbc.source.JdbcSourceTask;
import io.aiven.connect.jdbc.source.JdbcSourceTaskConfig;
import io.aiven.connect.jdbc.source.TableMonitorThread;
import io.aiven.connect.jdbc.util.CachedConnectionProvider;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.TableId;
import io.aiven.connect.jdbc.util.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JdbcConnector is a Kafka Connect Connector implementation that watches a JDBC database and
 * generates tasks to ingest database contents.
 */
public class JdbcSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);

    private static final long MAX_TIMEOUT = 10000L;

    private Map<String, String> configProperties;
    private JdbcSourceConnectorConfig config;
    private CachedConnectionProvider cachedConnectionProvider;
    private TableMonitorThread tableMonitorThread;
    private DatabaseDialect dialect;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> properties) throws ConnectException {
        log.info("Starting JDBC Source Connector");
        try {
            configProperties = properties;
            config = new JdbcSourceConnectorConfig(configProperties);
        } catch (final ConfigException e) {
            throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration error",
                e);
        }

        final String dbUrl = config.getConnectionUrl();
        final int maxConnectionAttempts = config.getInt(
            JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        );
        final long connectionRetryBackoff = config.getLong(
            JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        );
        dialect = DatabaseDialects.findBestFor(
            dbUrl,
            config
        );
        cachedConnectionProvider = new CachedConnectionProvider(
            dialect,
            maxConnectionAttempts,
            connectionRetryBackoff
        );

        // Initial connection attempt
        cachedConnectionProvider.getConnection();

        final long tablePollMs = config.getLong(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG);
        final List<String> whitelist = config.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
        Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
        final List<String> blacklist = config.getList(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
        final Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);
        final boolean qualifyTableNames = config.getBoolean(JdbcSourceConnectorConfig.TABLE_NAMES_QUALIFY_CONFIG);

        if (whitelistSet != null && blacklistSet != null) {
            throw new ConnectException(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
                + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " are "
                + "exclusive.");
        }
        final String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
        if (!query.isEmpty()) {
            if (whitelistSet != null || blacklistSet != null) {
                throw new ConnectException(JdbcSourceConnectorConfig.QUERY_CONFIG + " may not be combined"
                    + " with whole-table copying settings.");
            }
            // Force filtering out the entire set of tables since the one task we'll generate is for the
            // query.
            whitelistSet = Collections.emptySet();

        }
        final String mode = config.getMode();
        if (JdbcSourceConnectorConfig.MODE_INCREMENTING.equals(mode)
                || JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING.equals(mode)
        ) {
            final String incrementingColumn =
                    config.getString(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG);
            if (!qualifyTableNames && (incrementingColumn == null || incrementingColumn.isEmpty())) {
                // Otherwise, we may infer the wrong incrementing key for the table
                // TODO: This restraint is not necessary in all cases, but additional logic will be required to
                //       distinguish when it is and is not, and without that logic, the connector will fail to query
                //       tables by trying to read a non-existent column, which is likely to be very confusing to users.
                //       This is still technically possible even with explicitly-specified column names, but that
                //       can happen regardless of whether unqualified table names are used
                throw new ConnectException(
                        "When using unqualified table names and either " + JdbcSourceConnectorConfig.MODE_INCREMENTING
                                + " or " + JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING + " mode, an "
                                + "incrementing column name must be explicitly provided via the '"
                                + JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG + "' property."
                );
            }
        }
        tableMonitorThread = new TableMonitorThread(
            dialect,
            cachedConnectionProvider,
            context,
            tablePollMs,
            whitelistSet,
            blacklistSet,
            qualifyTableNames
        );
        tableMonitorThread.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
        if (!query.isEmpty()) {
            final List<Map<String, String>> taskConfigs = new ArrayList<>(1);
            final Map<String, String> taskProps = new HashMap<>(configProperties);
            taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
            taskConfigs.add(taskProps);
            log.trace("Task configs with no query");
            return taskConfigs;
        } else {
            final List<TableId> currentTables = tableMonitorThread.tables();
            final int numGroups = Math.min(currentTables.size(), maxTasks);
            final List<List<TableId>> tablesGrouped = ConnectorUtils.groupPartitions(currentTables, numGroups);
            final List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
            for (final List<TableId> taskTables : tablesGrouped) {
                final Map<String, String> taskProps = new HashMap<>(configProperties);
                final ExpressionBuilder builder = dialect.expressionBuilder();
                builder.appendList().delimitedBy(",").of(taskTables);
                taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, builder.toString());
                taskConfigs.add(taskProps);
            }
            log.trace("Task configs with query: {}, tables: {}", taskConfigs, currentTables.toArray());
            return taskConfigs;
        }
    }

    @Override
    public void stop() throws ConnectException {
        log.info("Stopping table monitoring thread");
        tableMonitorThread.shutdown();
        try {
            tableMonitorThread.join(MAX_TIMEOUT);
        } catch (final InterruptedException e) {
            // Ignore, shouldn't be interrupted
        } finally {
            try {
                cachedConnectionProvider.close();
            } finally {
                try {
                    if (dialect != null) {
                        dialect.close();
                    }
                } catch (final Throwable t) {
                    log.warn("Error while closing the {} dialect: ", dialect, t);
                } finally {
                    dialect = null;
                }
            }
        }
    }

    @Override
    public ConfigDef config() {
        return JdbcSourceConnectorConfig.CONFIG_DEF;
    }
}
