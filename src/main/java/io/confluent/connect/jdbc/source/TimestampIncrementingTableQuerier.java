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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.JdbcUtils;

/**
 * <p>
 *   TimestampIncrementingTableQuerier performs incremental loading of data using two mechanisms: a
 *   timestamp column provides monotonically incrementing values that can be used to detect new or
 *   modified rows and a strictly incrementing (e.g. auto increment) column allows detecting new rows
 *   or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 *   At least one of the two columns must be specified (or left as "" for the incrementing column
 *   to indicate use of an auto-increment column). If both columns are provided, they are both
 *   used to ensure only new or updated rows are reported and to totally order updates so
 *   recovery can occur no matter when offsets were committed. If only the incrementing fields is
 *   provided, new rows will be detected but not updates. If only the timestamp field is
 *   provided, both new and updated rows will be detected, but stream offsets will not be unique
 *   so failures may cause duplicates or losses.
 * </p>
 */
public class TimestampIncrementingTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(TimestampIncrementingTableQuerier.class);

  private static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

  private String timestampColumn;
  private String incrementingColumn;
  private long timestampDelay;
  private TimestampIncrementingOffset offset;

  public TimestampIncrementingTableQuerier(QueryMode mode, String name, String topicPrefix,
                                           String timestampColumn, String incrementingColumn,
                                           Map<String, Object> offsetMap, Long timestampDelay,
                                           String schemaPattern, boolean mapNumerics) {
    super(mode, name, topicPrefix, schemaPattern, mapNumerics);
    this.timestampColumn = timestampColumn;
    this.incrementingColumn = incrementingColumn;
    this.timestampDelay = timestampDelay;
    this.offset = TimestampIncrementingOffset.fromMap(offsetMap);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    // Default when unspecified uses an autoincrementing column
    if (incrementingColumn != null && incrementingColumn.isEmpty()) {
      incrementingColumn = JdbcUtils.getAutoincrementColumn(db, schemaPattern, name);
    }

    String quoteString = JdbcUtils.getIdentifierQuoteString(db);

    StringBuilder builder = new StringBuilder();

    switch (mode) {
      case TABLE:
        builder.append("SELECT * FROM ");
        builder.append(JdbcUtils.quoteString(name, quoteString));
        break;
      case QUERY:
        builder.append(query);
        break;
      default:
        throw new ConnectException("Unknown mode encountered when preparing query: " + mode.toString());
    }

    if (incrementingColumn != null && timestampColumn != null) {
      // This version combines two possible conditions. The first checks timestamp == last
      // timestamp and incrementing > last incrementing. The timestamp alone would include
      // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
      // get only the row with id = 23:
      //  timestamp 1234, id 22 <- last
      //  timestamp 1234, id 23
      // The second check only uses the timestamp >= last timestamp. This covers everything new,
      // even if it is an update of the existing row. If we previously had:
      //  timestamp 1234, id 22 <- last
      // and then these rows were written:
      //  timestamp 1235, id 22
      //  timestamp 1236, id 23
      // We should capture both id = 22 (an update) and id = 23 (a new row)
      builder.append(" WHERE ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" < ? AND ((");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" = ? AND ");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" > ?");
      builder.append(") OR ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" > ?)");
      builder.append(" ORDER BY ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(",");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" ASC");
    } else if (incrementingColumn != null) {
      builder.append(" WHERE ");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" > ?");
      builder.append(" ORDER BY ");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" ASC");
    } else if (timestampColumn != null) {
      builder.append(" WHERE ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" > ? AND ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" < ? ORDER BY ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" ASC");
    }
    String queryString = builder.toString();
    log.debug("{} prepared SQL query: {}", this, queryString);
    stmt = db.prepareStatement(queryString);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    if (incrementingColumn != null && timestampColumn != null) {
      Timestamp tsOffset = offset.getTimestampOffset();
      Long incOffset = offset.getIncrementingOffset();
      Timestamp endTime = new Timestamp(JdbcUtils.getCurrentTimeOnDB(stmt.getConnection(), DateTimeUtils.UTC_CALENDAR.get()).getTime() - timestampDelay);
      stmt.setTimestamp(1, endTime, DateTimeUtils.UTC_CALENDAR.get());
      stmt.setTimestamp(2, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
      stmt.setLong(3, incOffset);
      stmt.setTimestamp(4, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
      log.debug("Executing prepared statement with start time value = {} end time = {} and incrementing value = {}",
                DateTimeUtils.formatUtcTimestamp(tsOffset),
                DateTimeUtils.formatUtcTimestamp(endTime),
                incOffset);
    } else if (incrementingColumn != null) {
      Long incOffset = offset.getIncrementingOffset();
      stmt.setLong(1, incOffset);
      log.debug("Executing prepared statement with incrementing value = {}", incOffset);
    } else if (timestampColumn != null) {
      Timestamp tsOffset = offset.getTimestampOffset();
      Timestamp endTime = new Timestamp(JdbcUtils.getCurrentTimeOnDB(stmt.getConnection(), DateTimeUtils.UTC_CALENDAR.get()).getTime() - timestampDelay);
      stmt.setTimestamp(1, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
      stmt.setTimestamp(2, endTime, DateTimeUtils.UTC_CALENDAR.get());
      log.debug("Executing prepared statement with timestamp value = {} end time = {}",
                DateTimeUtils.formatUtcTimestamp(tsOffset),
                DateTimeUtils.formatUtcTimestamp(endTime));
    }
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    final Struct record = DataConverter.convertRecord(schema, resultSet, mapNumerics);
    offset = extractOffset(schema, record);
    // TODO: Key?
    final String topic;
    final Map<String, String> partition;
    switch (mode) {
      case TABLE:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
        topic = topicPrefix + name;
        break;
      case QUERY:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                             JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
        topic = topicPrefix;
        break;
      default:
        throw new ConnectException("Unexpected query mode: " + mode);
    }
    return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
  }

  // Visible for testing
  TimestampIncrementingOffset extractOffset(Schema schema, Struct record) {
    final Timestamp extractedTimestamp;
    if (timestampColumn != null) {
      extractedTimestamp = (Timestamp) record.get(timestampColumn);
      Timestamp timestampOffset = offset.getTimestampOffset();
      assert timestampOffset != null && timestampOffset.compareTo(extractedTimestamp) <= 0;
    } else {
      extractedTimestamp = null;
    }

    final Long extractedId;
    if (incrementingColumn != null) {
      final Schema incrementingColumnSchema = schema.field(incrementingColumn).schema();
      final Object incrementingColumnValue = record.get(incrementingColumn);
      if (incrementingColumnValue == null) {
        throw new ConnectException("Null value for incrementing column of type: " + incrementingColumnSchema.type());
      } else if (isIntegralPrimitiveType(incrementingColumnValue)) {
        extractedId = ((Number) incrementingColumnValue).longValue();
      } else if (incrementingColumnSchema.name() != null && incrementingColumnSchema.name().equals(Decimal.LOGICAL_NAME)) {
        final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
        if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
          throw new ConnectException("Decimal value for incrementing column exceeded Long.MAX_VALUE");
        }
        if (decimal.scale() != 0) {
          throw new ConnectException("Scale of Decimal value for incrementing column must be 0");
        }
        extractedId = decimal.longValue();
      } else {
        throw new ConnectException("Invalid type for incrementing column: " + incrementingColumnSchema.type());
      }

      // If we are only using an incrementing column, then this must be incrementing.
      // If we are also using a timestamp, then we may see updates to older rows.
      Long incrementingOffset = offset.getIncrementingOffset();
      assert incrementingOffset == -1L || extractedId > incrementingOffset || timestampColumn != null;
    } else {
      extractedId = null;
    }

    return new TimestampIncrementingOffset(extractedTimestamp, extractedId);
  }

  private boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
    return incrementingColumnValue instanceof Long
           || incrementingColumnValue instanceof Integer
           || incrementingColumnValue instanceof Short
           || incrementingColumnValue instanceof Byte;
  }

  @Override
  public String toString() {
    return "TimestampIncrementingTableQuerier{" +
           "name='" + name + '\'' +
           ", query='" + query + '\'' +
           ", topicPrefix='" + topicPrefix + '\'' +
           ", timestampColumn='" + timestampColumn + '\'' +
           ", incrementingColumn='" + incrementingColumn + '\'' +
           '}';
  }
}
