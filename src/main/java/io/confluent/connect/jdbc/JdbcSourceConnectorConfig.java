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

import java.util.Arrays;
import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

public class JdbcSourceConnectorConfig extends AbstractConfig {

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC = "JDBC connection URL for the database to load.";

  public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
  private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
                                                     + "each table.";
  public static final int POLL_INTERVAL_MS_DEFAULT = 5000;

  public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
  private static final String BATCH_MAX_ROWS_DOC =
      "Maximum number of rows to include in a single batch when polling for new data. This "
      + "setting can be used to limit the amount of data buffered internally in the connector.";
  public static final int BATCH_MAX_ROWS_DEFAULT = 100;

  public static final String MODE_CONFIG = "mode";
  private static final String MODE_DOC =
      "The mode for updating a table each time it is polled. Options include:\n"
      + "  * bulk - perform a bulk load of the entire table each time it is polled\n"
      + "  * increasing - use a strictly increasing column on each table to "
      + "detect only new rows. Note that this will not detect modifications or "
      + "deletions of existing rows.\n"
      + "  * timestamp - use a timestamp (or timestamp-like) column to detect new and modified "
      + "rows. This assumes the column is updated with each write, and that values are "
      + "monotonically increasing, but not necessarily unique.\n"
      + "  * timestamp+increasing - use two columns, a timestamp column that detects new and "
      + "modified rows and a strictly increasing column which provides a globally unique ID for "
      + "updates so each row can be assigned a unique stream offset.";
  public static final String MODE_DEFAULT = "bulk";

  public static final String MODE_BULK = "bulk";
  public static final String MODE_TIMESTAMP = "timestamp";
  public static final String MODE_INCREASING = "increasing";
  public static final String MODE_TIMESTAMP_INCREASING = "timestamp+increasing";

  public static final String INCREASING_COLUMN_NAME_CONFIG = "increasing.column.name";
  private static final String INCREASING_COLUMN_NAME_DOC =
      "The name of the strictly increasing column to use to detect new rows. Any empty value "
      + "indicates the column should be autodetected by looking for an auto-incrementing column.";
  public static final String INCREASING_COLUMN_NAME_DEFAULT = "";

  public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
  private static final String TIMESTAMP_COLUMN_NAME_DOC =
      "The name of the timestamp column to use to detect new or modified rows.";
  public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";

  public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
  private static final String TABLE_POLL_INTERVAL_MS_DOC =
      "Frequency in ms to poll for new or removed tables, which may result in updated task "
      + "configurations to start polling for data in added tables or stop polling for data in "
      + "removed tables.";
  public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 60 * 1000;

  static ConfigDef config = new ConfigDef()
      .define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC)
      .define(POLL_INTERVAL_MS_CONFIG, Type.INT, POLL_INTERVAL_MS_DEFAULT, Importance.HIGH,
              POLL_INTERVAL_MS_DOC)
      .define(BATCH_MAX_ROWS_CONFIG, Type.INT, BATCH_MAX_ROWS_DEFAULT, Importance.LOW,
              BATCH_MAX_ROWS_DOC)
      .define(MODE_CONFIG, Type.STRING, MODE_DEFAULT,
              ConfigDef.ValidString.in(Arrays.asList("bulk", "increasing", "timestamp",
                                                     "timestamp+increasing")),
              Importance.HIGH, MODE_DOC)
      .define(INCREASING_COLUMN_NAME_CONFIG, Type.STRING, INCREASING_COLUMN_NAME_DEFAULT,
              Importance.MEDIUM, INCREASING_COLUMN_NAME_DOC)
      .define(TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, TIMESTAMP_COLUMN_NAME_DEFAULT,
              Importance.MEDIUM, TIMESTAMP_COLUMN_NAME_DOC)
      .define(TABLE_POLL_INTERVAL_MS_CONFIG, Type.LONG, TABLE_POLL_INTERVAL_MS_DEFAULT,
              Importance.LOW, TABLE_POLL_INTERVAL_MS_DOC);

  JdbcSourceConnectorConfig(Map<String, String> props) {
    super(config, props);
  }
}
