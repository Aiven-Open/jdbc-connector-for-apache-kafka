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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * <p>
 *   TimestampIncreasingTableQuerier performs incremental loading of data using two mechanisms: a
 *   timestamp column provides monotonically increasing values that can be used to detect new or
 *   modified rows and a strictly increasing (e.g. auto increment) column allows detecting new rows
 *   or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 *   At least one of the two columns must be specified (or left as "" for the increasing column
 *   to indicate use of an auto-increment column). If both columns are provided, they are both
 *   used to ensure only new or updated rows are reported and to totally order updates so
 *   recovery can occur no matter when offsets were committed. If only the increasing fields is
 *   provided, new rows will be detected but not updates. If only the timestamp field is
 *   provided, both new and updated rows will be detected, but stream offsets will not be unique
 *   so failures may cause duplicates or losses.
 * </p>
 */
public class TimestampIncreasingTableQuerier extends TableQuerier {
  private static final Calendar UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

  private String timestampColumn;
  private Long timestampOffset;
  private String increasingColumn;
  private Long increasingOffset = null;

  public TimestampIncreasingTableQuerier(String name,
                                         String timestampColumn, Long timestampOffset,
                                         String increasingColumn, Long increasingOffset) {
    super(name);
    this.timestampColumn = timestampColumn;
    this.timestampOffset = timestampOffset;
    this.increasingColumn = increasingColumn;
    this.increasingOffset = increasingOffset;
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    // Default when unspecified uses an autoincrementing column
    if (increasingColumn != null && increasingColumn.isEmpty()) {
      increasingColumn = JdbcUtils.getAutoincrementColumn(db, name);
    }

    StringBuilder builder = new StringBuilder();
    builder.append("SELECT * FROM \"");
    builder.append(name);
    builder.append("\"");
    if (increasingColumn != null && timestampColumn != null) {
      // This version combines two possible conditions. The first checks timestamp == last
      // timestamp and increasing > last increasing. The timestamp alone would include
      // duplicates, but adding the increasing condition ensures no duplicates, e.g. you would
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
      builder.append(" WHERE (\"");
      builder.append(timestampColumn);
      builder.append("\" = ? AND \"");
      builder.append(increasingColumn);
      builder.append("\" > ?");
      builder.append(") OR \"");
      builder.append(timestampColumn);
      builder.append("\" > ?");
      builder.append(" ORDER BY \"");
      builder.append(timestampColumn);
      builder.append("\",\"");
      builder.append(increasingColumn);
      builder.append("\" ASC");
    } else if (increasingColumn != null) {
      builder.append(" WHERE \"");
      builder.append(increasingColumn);
      builder.append("\" > ?");
      builder.append(" ORDER BY \"");
      builder.append(increasingColumn);
      builder.append("\" ASC");
    } else if (timestampColumn != null) {
      builder.append(" WHERE \"");
      builder.append(timestampColumn);
      builder.append("\" > ?");
      builder.append(" ORDER BY \"");
      builder.append(timestampColumn);
      builder.append("\" ASC");
    }

    stmt = db.prepareStatement(builder.toString());
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    if (increasingColumn != null && timestampColumn != null) {
      Timestamp ts = new Timestamp(timestampOffset == null ? 0 : timestampOffset);
      stmt.setTimestamp(1, ts, UTC_CALENDAR);
      stmt.setLong(2, (increasingOffset == null ? -1 : increasingOffset));
      stmt.setTimestamp(3, ts, UTC_CALENDAR);
    } else if (increasingColumn != null) {
      stmt.setLong(1, (increasingOffset == null ? -1 : increasingOffset));
    } else if (timestampColumn != null) {
      Timestamp ts = new Timestamp(timestampOffset == null ? 0 : timestampOffset);
      stmt.setTimestamp(1, ts, UTC_CALENDAR);
    }
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = DataConverter.convertRecord(schema, resultSet);
    Map<String, Long> offset = new HashMap<>();
    if (increasingColumn != null) {
      Long id;
      switch (schema.field(increasingColumn).schema().type()) {
        case INT32:
          id = (long) (Integer) record.get(increasingColumn);
          break;
        case INT64:
          id = (Long) record.get(increasingColumn);
          break;
        default:
          throw new ConnectException("Invalid type for increasing column: "
                                            + schema.field(increasingColumn).schema().type());
      }

      // If we are only using an increasing column, then this must be increasing. If we are also
      // using a timestamp, then we may see updates to older rows.
      assert (increasingOffset == null || id > increasingOffset) || timestampColumn != null;
      increasingOffset = id;

      offset.put(JdbcSourceTask.INCREASING_FIELD, id);
    }


    if (timestampColumn != null) {
      Date timestamp = (Date) record.get(timestampColumn);
      assert timestampOffset == null || timestamp.getTime() >= timestampOffset;
      timestampOffset = timestamp.getTime();
      offset.put(JdbcSourceTask.TIMESTAMP_FIELD, timestampOffset);
    }

    // TODO: Key?
    Map<String, String> partition =
        Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
    return new SourceRecord(partition, offset, name, null, null, record);
  }
}
