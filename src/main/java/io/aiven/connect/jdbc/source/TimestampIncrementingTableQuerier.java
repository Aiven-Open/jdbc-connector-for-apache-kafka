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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.source.SchemaMapping.FieldSetter;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.DateTimeUtils;
import io.aiven.connect.jdbc.util.ExpressionBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * TimestampIncrementingTableQuerier performs incremental loading of data using two mechanisms: a
 * timestamp column provides monotonically incrementing values that can be used to detect new or
 * modified rows and a strictly incrementing (e.g. auto increment) column allows detecting new
 * rows or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 * At least one of the two columns must be specified (or left as "" for the incrementing column
 * to indicate use of an auto-increment column). If both columns are provided, they are both
 * used to ensure only new or updated rows are reported and to totally order updates so
 * recovery can occur no matter when offsets were committed. If only the incrementing fields is
 * provided, new rows will be detected but not updates. If only the timestamp field is
 * provided, both new and updated rows will be detected, but stream offsets will not be unique
 * so failures may cause duplicates or losses.
 * </p>
 */
public class TimestampIncrementingTableQuerier extends TableQuerier
    implements TimestampIncrementingCriteria.CriteriaValues {
    private static final Logger log = LoggerFactory.getLogger(
        TimestampIncrementingTableQuerier.class
    );

    private final List<String> timestampColumnNames;
    private final List<ColumnId> timestampColumns;
    private String incrementingColumnName;
    private long timestampDelay;
    private final long initialTimestampOffset;
    private final long initialIncrementingOffset;
    private TimestampIncrementingOffset offset;
    private TimestampIncrementingCriteria criteria;
    private final Map<String, String> partition;
    private final String topic;
    private final TimeZone timeZone;

    public TimestampIncrementingTableQuerier(final DatabaseDialect dialect,
                                             final QueryMode mode,
                                             final String name,
                                             final String topicPrefix,
                                             final List<String> timestampColumnNames,
                                             final String incrementingColumnName,
                                             final Map<String, Object> offsetMap,
                                             final Long timestampDelay,
                                             final long timestampInitialMs,
                                             final long incrementingOffsetInitial,
                                             final TimeZone timeZone) {
        super(dialect, mode, name, topicPrefix);
        this.incrementingColumnName = incrementingColumnName;
        this.timestampColumnNames = timestampColumnNames != null
            ? timestampColumnNames : Collections.<String>emptyList();
        this.timestampDelay = timestampDelay;
        this.initialTimestampOffset = timestampInitialMs;
        this.initialIncrementingOffset = incrementingOffsetInitial;
        this.offset = TimestampIncrementingOffset.fromMap(offsetMap);

        this.timestampColumns = new ArrayList<>();
        for (final String timestampColumn : this.timestampColumnNames) {
            if (timestampColumn != null && !timestampColumn.isEmpty()) {
                timestampColumns.add(new ColumnId(tableId, timestampColumn));
            }
        }

        switch (mode) {
            case TABLE:
                final String tableName = tableId.tableName();
                topic = topicPrefix + tableName; // backward compatible
                partition = OffsetProtocols.sourcePartitionForProtocolV1(tableId);
                break;
            case QUERY:
                partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                    JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
                topic = topicPrefix;
                break;
            default:
                throw new ConnectException("Unexpected query mode: " + mode);
        }

        this.timeZone = timeZone;
    }

    @Override
    protected void createPreparedStatement(final Connection db) throws SQLException {
        findDefaultAutoIncrementingColumn(db);

        ColumnId incrementingColumn = null;
        if (incrementingColumnName != null && !incrementingColumnName.isEmpty()) {
            incrementingColumn = new ColumnId(tableId, incrementingColumnName);
        }

        final ExpressionBuilder builder = dialect.expressionBuilder();
        switch (mode) {
            case TABLE:
                builder.append("SELECT * FROM ");
                builder.append(tableId);
                break;
            case QUERY:
                builder.append(query);
                break;
            default:
                throw new ConnectException(
                    "Unknown mode encountered when preparing query: " + mode);
        }

        // Append the criteria using the columns ...
        criteria = dialect.criteriaFor(incrementingColumn, timestampColumns);
        criteria.whereClause(builder);

        final String queryString = builder.toString();
        log.debug("{} prepared SQL query: {}", this, queryString);
        stmt = dialect.createPreparedStatement(db, queryString);
    }

    private void findDefaultAutoIncrementingColumn(final Connection db) throws SQLException {
        // Default when unspecified uses an autoincrementing column
        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            // Find the first auto-incremented column ...
            for (final ColumnDefinition defn : dialect.describeColumns(
                db,
                tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(),
                null).values()) {
                if (defn.isAutoIncrement()) {
                    incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }
        // If still not found, query the table and use the result set metadata.
        // This doesn't work if the table is empty.
        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            log.debug("Falling back to describe '{}' table by querying {}", tableId, db);
            for (final ColumnDefinition defn : dialect.describeColumnsByQuerying(db, tableId).values()) {
                if (defn.isAutoIncrement()) {
                    incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        criteria.setQueryParameters(stmt, this);
        return stmt.executeQuery();
    }

    @Override
    public SourceRecord extractRecord() throws SQLException {
        final Struct record = new Struct(schemaMapping.schema());
        for (final FieldSetter setter : schemaMapping.fieldSetters()) {
            try {
                setter.setField(record, resultSet);
            } catch (final IOException e) {
                log.warn("Ignoring record because processing failed:", e);
            } catch (final SQLException e) {
                log.warn("Ignoring record due to SQL error:", e);
            }
        }
        offset = criteria.extractValues(schemaMapping.schema(), record, offset);
        return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
    }

    @Override
    public Timestamp beginTimestampValue() {
        final Timestamp timestampOffset = offset.getTimestampOffset();
        if (timestampOffset != null) {
            return timestampOffset;
        } else {
            return new Timestamp(initialTimestampOffset);
        }
    }

    @Override
    public Timestamp endTimestampValue() throws SQLException {
        final long currentDbTime = dialect.currentTimeOnDB(
            stmt.getConnection(),
            DateTimeUtils.getTimeZoneCalendar(timeZone)
        ).getTime();
        return new Timestamp(currentDbTime - timestampDelay);
    }

    @Override
    public Long lastIncrementedValue() {
        final Long incrementingOffset = offset.getIncrementingOffset();
        if (incrementingOffset != null) {
            return incrementingOffset;
        } else {
            return initialIncrementingOffset;
        }
    }

    @Override
    public String toString() {
        return "TimestampIncrementingTableQuerier{"
            + "table=" + tableId
            + ", query='" + query + '\''
            + ", topicPrefix='" + topicPrefix + '\''
            + ", incrementingColumn='" + (incrementingColumnName != null
            ? incrementingColumnName
            : "") + '\''
            + ", timestampColumns=" + timestampColumnNames
            + '}';
    }
}
