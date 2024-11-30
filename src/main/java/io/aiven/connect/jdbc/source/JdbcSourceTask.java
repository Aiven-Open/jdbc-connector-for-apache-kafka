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

    // Visible for testing
    public static final int MAX_QUERY_SLEEP_MS = 100;

    private final Time time;
    private JdbcSourceTaskConfig config;
    private DatabaseDialect dialect;
    private CachedConnectionProvider cachedConnectionProvider;
    private final PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Object pollLock = new Object();

    public JdbcSourceTask() {
        this.time = Time.SYSTEM;
    }

    public JdbcSourceTask(final Time time) {
        this.time = time;
    }

    // Smaller conditions for readability
    private boolean isModeIncrementing(String mode) {
        return mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING);
    }

    private boolean isModeTimestamp(String mode) {
        return mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP);
    }

    private boolean isModeTimestampIncrementing(String mode) {
        return mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING);
    }

    private boolean isIncrementingOptionalMode(String incrementalMode) {
        return incrementalMode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING)
                || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING);
    }

    private boolean isTimestampOptionalMode(String incrementalMode) {
        return incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP)
                || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING);
    }

    // Composite validation methods
    private boolean isSupportedTaskMode(String mode) {
        return isModeIncrementing(mode) || isModeTimestamp(mode) || isModeTimestampIncrementing(mode);
    }

    private boolean isIncrementingModeOptional(String incrementalMode, boolean incrementingOptional) {
        return isIncrementingOptionalMode(incrementalMode) && incrementingOptional;
    }

    private boolean isTimestampModeNotOptional(String incrementalMode, boolean atLeastOneTimestampNotOptional) {
        return isTimestampOptionalMode(incrementalMode) && !atLeastOneTimestampNotOptional;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    // Extract method validateConfig, initializeDialect, processQueryMode, getTablePartitionsToCheck, processOffsets
    // addTableQuerier from original start.
    @Override
    public void start(final Map<String, String> properties) {
        log.info("Starting JDBC source task");

        // Validate and initialize configuration
        validateConfig(properties);

        // Initialize the JDBC dialect (specific database dialect)
        initializeDialect();

        // Create the connection provider with the required configurations
        cachedConnectionProvider = new SourceConnectionProvider(
                dialect,
                config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG),
                config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG)
        );

        // Retrieve table names and custom query from config
        final List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
        final String query = config.getString(JdbcSourceTaskConfig.QUERY_CONFIG);

        // Determine the query mode (either TABLE mode or QUERY mode)
        final TableQuerier.QueryMode queryMode = !query.isEmpty() ? TableQuerier.QueryMode.QUERY : TableQuerier.QueryMode.TABLE;
        final List<String> tablesOrQuery = queryMode == TableQuerier.QueryMode.QUERY ? Collections.singletonList(query) : tables;

        // Handle partitions and offsets based on the query mode
        Map<Map<String, String>, Map<String, Object>> offsets = null;
        final String mode = config.getMode();
        if (isSupportedTaskMode(mode)) {
            offsets = processQueryMode(tables, mode, queryMode, new HashMap<>());
        }

        // Get additional config values like columns for incrementing, timestamps, and validation
        final String incrementingColumn = config.getString(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
        final List<String> timestampColumns = config.getList(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
        final Long timestampDelayInterval = config.getLong(JdbcSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
        final Long timestampInitialMs = config.getLong(JdbcSourceTaskConfig.TIMESTAMP_INITIAL_MS_CONFIG);
        final Long incrementingOffsetInitial = config.getLong(JdbcSourceTaskConfig.INCREMENTING_INITIAL_VALUE_CONFIG);
        final boolean validateNonNulls = config.getBoolean(JdbcSourceTaskConfig.VALIDATE_NON_NULL_CONFIG);

        // Iterate over each table or query and process them based on the query mode
        for (final String tableOrQuery : tablesOrQuery) {
            // Determine the partitions to check for the current table/query
            final List<Map<String, String>> tablePartitionsToCheck = getTablePartitionsToCheck(
                    queryMode, tableOrQuery, validateNonNulls, incrementingColumn, timestampColumns
            );

            // Process offsets for the partitions
            Map<String, Object> offset = processOffsets(offsets, tablePartitionsToCheck);

            // Add the appropriate TableQuerier to the queue based on the mode (bulk, incrementing, timestamp)
            addTableQuerier(
                    queryMode, mode, tableOrQuery, offset, config.getString(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG),
                    timestampColumns, incrementingColumn, timestampDelayInterval, timestampInitialMs, incrementingOffsetInitial
            );
        }

        // Mark the task as running
        running.set(true);
        log.info("Started JDBC source task");
    }

    /**
     * Validates the configuration for the task and initializes the config object.
     * Throws a ConnectException if there is an issue with the configuration.
     */
    private void validateConfig(Map<String, String> properties) {
        try {
            config = new JdbcSourceTaskConfig(properties);
            config.validate();
        } catch (final ConfigException e) {
            throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
        }
    }

    /**
     * Initializes the JDBC dialect based on the provided dialect name or connection URL.
     * The dialect determines how SQL queries are formed for the specific database.
     */
    private void initializeDialect() {
        final String dialectName = config.getDialectName();
        if (dialectName != null && !dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(dialectName, config);
        } else {
            final String connectionUrl = config.getConnectionUrl();
            dialect = DatabaseDialects.findBestFor(connectionUrl, config);
        }
        log.info("Using JDBC dialect {}", dialect.name());
    }

    /**
     * Processes the query mode (TABLE or QUERY) and determines the table partitions to query.
     * Retrieves the offsets for each partition if the mode is supported.
     */
    private Map<Map<String, String>, Map<String, Object>> processQueryMode(
            List<String> tables, String mode, TableQuerier.QueryMode queryMode,
            Map<String, List<Map<String, String>>> partitionsByTableFqn) {

        final List<Map<String, String>> partitions = new ArrayList<>(tables.size());
        switch (queryMode) {
            case TABLE:
                log.trace("Starting in TABLE mode");
                // Find partitions for each table in TABLE mode
                for (final String table : tables) {
                    final List<Map<String, String>> tablePartitions = possibleTablePartitions(table);
                    partitions.addAll(tablePartitions);
                    partitionsByTableFqn.put(table, tablePartitions);
                }
                break;
            case QUERY:
                log.trace("Starting in QUERY mode");
                // In QUERY mode, we only have one partition to check
                partitions.add(Collections.singletonMap(
                        JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                        JdbcSourceConnectorConstants.QUERY_NAME_VALUE
                ));
                break;
            default:
                throw new ConnectException("Unknown query mode: " + queryMode);
        }
        return context.offsetStorageReader().offsets(partitions);
    }

    /**
     * Retrieves the list of table partitions to check based on the query mode.
     * If validation of non-null values is enabled, it will validate the table.
     */
    private List<Map<String, String>> getTablePartitionsToCheck(
            TableQuerier.QueryMode queryMode, String tableOrQuery, boolean validateNonNulls,
            String incrementingColumn, List<String> timestampColumns) {

        final List<Map<String, String>> tablePartitionsToCheck;
        if (queryMode == TableQuerier.QueryMode.TABLE) {
            if (validateNonNulls) {
                validateNonNullable(config.getMode(), tableOrQuery, incrementingColumn, timestampColumns);
            }
            tablePartitionsToCheck = possibleTablePartitions(tableOrQuery);
        } else {
            // In QUERY mode, we add a single partition for the query
            tablePartitionsToCheck = Collections.singletonList(Collections.singletonMap(
                    JdbcSourceConnectorConstants.QUERY_NAME_KEY, JdbcSourceConnectorConstants.QUERY_NAME_VALUE
            ));
        }
        return tablePartitionsToCheck;
    }

    /**
     * Processes the offsets based on the provided partitions and returns the corresponding offset for the partition.
     */
    private Map<String, Object> processOffsets(Map<Map<String, String>, Map<String, Object>> offsets, List<Map<String, String>> tablePartitionsToCheck) {
        Map<String, Object> offset = null;
        if (offsets != null) {
            // Iterate over the partitions and retrieve the offset for the partition
            for (final Map<String, String> toCheckPartition : tablePartitionsToCheck) {
                offset = offsets.get(toCheckPartition);
                if (offset != null) {
                    log.info("Found offset {} for partition {}", offsets, toCheckPartition);
                    break;
                }
            }
        }
        return offset;
    }

    /**
     * Adds the appropriate TableQuerier (Bulk, Incrementing, or Timestamp-based) to the task queue.
     * Depending on the mode, different queriers are added.
     */
    private void addTableQuerier(
            TableQuerier.QueryMode queryMode, String mode, String tableOrQuery,
            Map<String, Object> offset, String topicPrefix, List<String> timestampColumns,
            String incrementingColumn, Long timestampDelayInterval, Long timestampInitialMs, Long incrementingOffsetInitial) {

        // Add a BulkTableQuerier for BULK mode
        if (mode.equals(JdbcSourceTaskConfig.MODE_BULK)) {
            tableQueue.add(new BulkTableQuerier(dialect, queryMode, tableOrQuery, topicPrefix));
        }
        // Add a TimestampIncrementingTableQuerier for INCREMENTING mode
        else if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)) {
            tableQueue.add(new TimestampIncrementingTableQuerier(dialect, queryMode, tableOrQuery, topicPrefix, null, incrementingColumn, offset, timestampDelayInterval, timestampInitialMs, incrementingOffsetInitial, config.getDBTimeZone()));
        }
        // Add a TimestampIncrementingTableQuerier for TIMESTAMP mode
        else if (mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)) {
            tableQueue.add(new TimestampIncrementingTableQuerier(dialect, queryMode, tableOrQuery, topicPrefix, timestampColumns, null, offset, timestampDelayInterval, timestampInitialMs, incrementingOffsetInitial, config.getDBTimeZone()));
        }
        // Add a TimestampIncrementingTableQuerier for TIMESTAMP_INCREMENTING mode
        else if (mode.endsWith(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
            tableQueue.add(new TimestampIncrementingTableQuerier(dialect, queryMode, tableOrQuery, topicPrefix, timestampColumns, incrementingColumn, offset, timestampDelayInterval, timestampInitialMs, incrementingOffsetInitial, config.getDBTimeZone()));
        }
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
        // Wait for any in-progress polls to stop before closing resources
        // On older versions of Kafka Connect, SourceTask::stop and SourceTask::poll may
        // be called concurrently on different threads
        // On more recent versions, SourceTask::stop is always called after the last invocation
        // of SourceTask::poll
        synchronized (pollLock) {
            closeResources();
        }
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
        synchronized (pollLock) {
            log.trace("Polling for new data");

            while (running.get()) {
                final TableQuerier querier = tableQueue.peek();

                if (!querier.querying()) {
                    // If not in the middle of an update, wait for next update time
                    final long nextUpdate = querier.getLastUpdate()
                        + config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
                    final long untilNext = nextUpdate - time.milliseconds();
                    final long sleepMs = Math.min(untilNext, MAX_QUERY_SLEEP_MS);
                    if (sleepMs > 0) {
                        log.trace("Waiting {} ms to poll {} next ({} ms total left to wait)",
                            sleepMs, querier.toString(), untilNext);
                        time.sleep(sleepMs);
                        // Return control to the Connect runtime periodically
                        // See https://kafka.apache.org/37/javadoc/org/apache/kafka/connect/source/SourceTask.html#poll():
                        // "If no data is currently available, this method should block but return control to the caller
                        // regularly (by returning null)"
                        return null;
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
            return null;
        }
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
            boolean timestampRequired = false;
            final Connection conn = cachedConnectionProvider.getConnection();
            final Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(conn, table, null);
            for (final ColumnDefinition defn : defnsById.values()) {
                final String columnName = defn.id().name();
                if (columnName.equalsIgnoreCase(incrementingColumn)) {
                    incrementingOptional = defn.isOptional();
                } else if (lowercaseTsColumns.contains(columnName.toLowerCase(Locale.getDefault()))) {
                    if (!defn.isOptional()) {
                        timestampRequired = true;
                    }
                }
            }

            // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
            // for table-based copying because custom query mode doesn't allow this to be looked up
            // without a query or parsing the query since we don't have a table name.
            if (isIncrementingModeOptional(incrementalMode, incrementingOptional)) {
                String errorMessage = String.format(
                        "Cannot make incremental queries using incrementing column %s on %s because this column is nullable.",
                        incrementingColumn, table
                );
                throw new ConnectException(errorMessage);
            }

            if (isTimestampModeNotOptional(incrementalMode, timestampRequired)) {
                String errorMessage = String.format(
                        "Cannot make incremental queries using incrementing column %s on %s because all these columns nullable.",
                        timestampColumns, table
                );
                throw new ConnectException(errorMessage);
            }
        } catch (final SQLException e) {
            throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                + " NULL", e);
        }
    }
}