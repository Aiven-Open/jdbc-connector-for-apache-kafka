/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.connect.jdbc.source;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialects;
import io.aiven.connect.jdbc.util.CachedConnectionProvider;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.TableId;
import io.aiven.connect.jdbc.util.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class JdbcSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

    private Time time;
    private JdbcSourceTaskConfig config;
    private DatabaseDialect dialect;
    private CachedConnectionProvider cachedConnectionProvider;
    private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public JdbcSourceTask() {
        this.time = Time.SYSTEM;
    }

    public JdbcSourceTask(final Time time) {
        this.time = time;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> properties) {
        log.info("Starting JDBC source task");
        try {
            config = new JdbcSourceTaskConfig(properties);
        } catch (final ConfigException e) {
            throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
        }

        final int maxConnAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long retryBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);

        final String dialectName = config.getDialectName();
        if (dialectName != null && !dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(dialectName, config);
        } else {
            final String connectionUrl = config.getConnectionUrl();
            dialect = DatabaseDialects.findBestFor(connectionUrl, config);
        }
        log.info("Using JDBC dialect {}", dialect.name());

        cachedConnectionProvider = new SourceConnectionProvider(dialect, maxConnAttempts, retryBackoff);

        final List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
        final String query = config.getString(JdbcSourceTaskConfig.QUERY_CONFIG);
        if ((tables.isEmpty() && query.isEmpty()) || (!tables.isEmpty() && !query.isEmpty())) {
            throw new ConnectException("Invalid configuration: each JdbcSourceTask must have at "
                + "least one table assigned to it or one query specified");
        }
        final TableQuerier.QueryMode queryMode = !query.isEmpty()
            ? TableQuerier.QueryMode.QUERY
            : TableQuerier.QueryMode.TABLE;
        final List<String> tablesOrQuery = queryMode == TableQuerier.QueryMode.QUERY
            ? Collections.singletonList(query)
            : tables;

        final String mode = config.getMode();
        //used only in table mode
        final Map<String, List<Map<String, String>>> partitionsByTableFqn = new HashMap<>();
        Map<Map<String, String>, Map<String, Object>> offsets = null;
        if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)
            || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)
            || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
            final List<Map<String, String>> partitions = new ArrayList<>(tables.size());
            switch (queryMode) {
                case TABLE:
                    log.trace("Starting in TABLE mode");
                    for (final String table : tables) {
                        // Find possible partition maps for different offset protocols
                        // We need to search by all offset protocol partition keys to support compatibility
                        final List<Map<String, String>> tablePartitions = possibleTablePartitions(table);
                        partitions.addAll(tablePartitions);
                        partitionsByTableFqn.put(table, tablePartitions);
                    }
                    break;
                case QUERY:
                    log.trace("Starting in QUERY mode");
                    partitions.add(Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                        JdbcSourceConnectorConstants.QUERY_NAME_VALUE));
                    break;
                default:
                    throw new ConnectException("Unknown query mode: " + queryMode);
            }
            offsets = context.offsetStorageReader().offsets(partitions);
            log.trace("The partition offsets are {}", offsets);
        }

        final String incrementingColumn
            = config.getString(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
        final List<String> timestampColumns
            = config.getList(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
        final Long timestampDelayInterval
            = config.getLong(JdbcSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
        final boolean validateNonNulls
            = config.getBoolean(JdbcSourceTaskConfig.VALIDATE_NON_NULL_CONFIG);

        for (final String tableOrQuery : tablesOrQuery) {
            final List<Map<String, String>> tablePartitionsToCheck;
            final Map<String, String> partition;
            switch (queryMode) {
                case TABLE:
                    if (validateNonNulls) {
                        validateNonNullable(
                            mode,
                            tableOrQuery,
                            incrementingColumn,
                            timestampColumns
                        );
                    }
                    tablePartitionsToCheck = partitionsByTableFqn.get(tableOrQuery);
                    break;
                case QUERY:
                    partition = Collections.singletonMap(
                        JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                        JdbcSourceConnectorConstants.QUERY_NAME_VALUE
                    );
                    tablePartitionsToCheck = Collections.singletonList(partition);
                    break;
                default:
                    throw new ConnectException("Unexpected query mode: " + queryMode);
            }

            // The partition map varies by offset protocol. Since we don't know which protocol each
            // table's offsets are keyed by, we need to use the different possible partitions
            // (newest protocol version first) to find the actual offsets for each table.
            Map<String, Object> offset = null;
            if (offsets != null) {
                for (final Map<String, String> toCheckPartition : tablePartitionsToCheck) {
                    offset = offsets.get(toCheckPartition);
                    if (offset != null) {
                        log.info("Found offset {} for partition {}", offsets, toCheckPartition);
                        break;
                    }
                }
            }

            final String topicPrefix = config.getString(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG);

            if (mode.equals(JdbcSourceTaskConfig.MODE_BULK)) {
                tableQueue.add(
                    new BulkTableQuerier(dialect, queryMode, tableOrQuery, topicPrefix)
                );
            } else if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)) {
                tableQueue.add(
                    new TimestampIncrementingTableQuerier(
                        dialect,
                        queryMode,
                        tableOrQuery,
                        topicPrefix,
                        null,
                        incrementingColumn,
                        offset,
                        timestampDelayInterval,
                        config.getDBTimeZone()
                    )
                );
            } else if (mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)) {
                tableQueue.add(
                    new TimestampIncrementingTableQuerier(
                        dialect,
                        queryMode,
                        tableOrQuery,
                        topicPrefix,
                        timestampColumns,
                        null,
                        offset,
                        timestampDelayInterval,
                        config.getDBTimeZone()
                    )
                );
            } else if (mode.endsWith(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
                tableQueue.add(
                    new TimestampIncrementingTableQuerier(
                        dialect,
                        queryMode,
                        tableOrQuery,
                        topicPrefix,
                        timestampColumns,
                        incrementingColumn,
                        offset,
                        timestampDelayInterval,
                        config.getDBTimeZone()
                    )
                );
            }
        }

        running.set(true);
        log.info("Started JDBC source task");
    }

    //This method returns a list of possible partition maps for different offset protocols
    //This helps with the upgrades
    private List<Map<String, String>> possibleTablePartitions(final String table) {
        final TableId tableId = dialect.parseTableIdentifier(table);
        return Arrays.asList(
            OffsetProtocols.sourcePartitionForProtocolV1(tableId),
            OffsetProtocols.sourcePartitionForProtocolV0(tableId)
        );
    }

    @Override
    public void stop() throws ConnectException {
        log.info("Stopping JDBC source task");
        running.set(false);
        // All resources are closed at the end of 'poll()' when no longer running or
        // if there is an error
    }

    protected void closeResources() {
        log.info("Closing resources for JDBC source task");
        try {
            if (cachedConnectionProvider != null) {
                cachedConnectionProvider.close();
            }
        } catch (final Throwable t) {
            log.warn("Error while closing the connections", t);
        } finally {
            cachedConnectionProvider = null;
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (final Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.trace("Polling for new data");

        while (running.get()) {
            final TableQuerier querier = tableQueue.peek();

            if (!querier.querying()) {
                // If not in the middle of an update, wait for next update time
                final long nextUpdate = querier.getLastUpdate()
                    + config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
                final long untilNext = nextUpdate - time.milliseconds();
                final long sleepMs = Math.min(untilNext, 100);
                if (sleepMs > 0) {
                    log.trace("Waiting {} ms to poll {} next ({} ms total left to wait)",
                        sleepMs, querier.toString(), untilNext);
                    time.sleep(sleepMs);
                    continue; // Re-check stop flag before continuing
                }
            }

            final List<SourceRecord> results = new ArrayList<>();
            try {
                log.debug("Checking for next block of results from {}", querier.toString());
                querier.maybeStartQuery(cachedConnectionProvider.getConnection());

                final int batchMaxRows = config.getInt(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
                boolean hadNext = true;
                while (results.size() < batchMaxRows && (hadNext = querier.next())) {
                    results.add(querier.extractRecord());
                }

                if (!hadNext) {
                    // If we finished processing the results from the current query, we can reset and send
                    // the querier to the tail of the queue
                    resetAndRequeueHead(querier);
                }

                if (results.isEmpty()) {
                    log.trace("No updates for {}", querier.toString());
                    continue;
                }

                log.debug("Returning {} records for {}", results.size(), querier.toString());
                return results;
            } catch (final SQLException sqle) {
                log.error("Failed to run query for table {}: {}", querier.toString(), sqle);
                resetAndRequeueHead(querier);
                return null;
            } catch (final Throwable t) {
                resetAndRequeueHead(querier);
                // This task has failed, so close any resources (may be reopened if needed) before throwing
                closeResources();
                throw t;
            }
        }

        // Only in case of shutdown
        final TableQuerier querier = tableQueue.peek();
        if (querier != null) {
            resetAndRequeueHead(querier);
        }
        closeResources();
        return null;
    }

    private void resetAndRequeueHead(final TableQuerier expectedHead) {
        log.debug("Resetting querier {}", expectedHead.toString());
        final TableQuerier removedQuerier = tableQueue.poll();
        assert removedQuerier == expectedHead;
        expectedHead.reset(time.milliseconds());
        tableQueue.add(expectedHead);
    }

    private void validateNonNullable(
        final String incrementalMode,
        final String table,
        final String incrementingColumn,
        final List<String> timestampColumns
    ) {
        try {
            final Set<String> lowercaseTsColumns = new HashSet<>();
            for (final String timestampColumn : timestampColumns) {
                lowercaseTsColumns.add(timestampColumn.toLowerCase(Locale.getDefault()));
            }

            boolean incrementingOptional = false;
            boolean atLeastOneTimestampNotOptional = false;
            final Connection conn = cachedConnectionProvider.getConnection();
            final Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(conn, table, null);
            for (final ColumnDefinition defn : defnsById.values()) {
                final String columnName = defn.id().name();
                if (columnName.equalsIgnoreCase(incrementingColumn)) {
                    incrementingOptional = defn.isOptional();
                } else if (lowercaseTsColumns.contains(columnName.toLowerCase(Locale.getDefault()))) {
                    if (!defn.isOptional()) {
                        atLeastOneTimestampNotOptional = true;
                    }
                }
            }

            // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
            // for table-based copying because custom query mode doesn't allow this to be looked up
            // without a query or parsing the query since we don't have a table name.
            if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING)
                || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
                && incrementingOptional) {
                throw new ConnectException("Cannot make incremental queries using incrementing column "
                    + incrementingColumn + " on " + table + " because this column "
                    + "is nullable.");
            }
            if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP)
                || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
                && !atLeastOneTimestampNotOptional) {
                throw new ConnectException("Cannot make incremental queries using timestamp columns "
                    + timestampColumns + " on " + table + " because all of these "
                    + "columns "
                    + "nullable.");
            }
        } catch (final SQLException e) {
            throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                + " NULL", e);
        }
    }
}
