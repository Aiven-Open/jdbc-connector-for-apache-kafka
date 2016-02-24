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
import io.confluent.common.config.ConfigException;

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
      + "  * incrementing - use a strictly incrementing column on each table to "
      + "detect only new rows. Note that this will not detect modifications or "
      + "deletions of existing rows.\n"
      + "  * timestamp - use a timestamp (or timestamp-like) column to detect new and modified "
      + "rows. This assumes the column is updated with each write, and that values are "
      + "monotonically incrementing, but not necessarily unique.\n"
      + "  * timestamp+incrementing - use two columns, a timestamp column that detects new and "
      + "modified rows and a strictly incrementing column which provides a globally unique ID for "
      + "updates so each row can be assigned a unique stream offset.";

  public static final String MODE_UNSPECIFIED = "";
  public static final String MODE_BULK = "bulk";
  public static final String MODE_TIMESTAMP = "timestamp";
  public static final String MODE_INCREMENTING = "incrementing";
  public static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

  public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
  private static final String INCREMENTING_COLUMN_NAME_DOC =
      "The name of the strictly incrementing column to use to detect new rows. Any empty value "
      + "indicates the column should be autodetected by looking for an auto-incrementing column. "
      + "This column may not be nullable.";
  public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";

  public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
  private static final String TIMESTAMP_COLUMN_NAME_DOC =
      "The name of the timestamp column to use to detect new or modified rows. This column may "
      + "not be nullable.";
  public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";

  public static final String TIMESTAMP_DELAY_INTERVAL_MS_CONFIG = "timestamp.delay.interval.ms";
  private static final String TIMESTAMP_DELAY_INTERVAL_MS_DOC = "How long to wait after a row with certain timestamp appears before we include it in the result. You may choose to add some delay to allow transactions with earlier timestamp to complete.";
  public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 0;



  public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
  private static final String TABLE_POLL_INTERVAL_MS_DOC =
      "Frequency in ms to poll for new or removed tables, which may result in updated task "
      + "configurations to start polling for data in added tables or stop polling for data in "
      + "removed tables.";
  public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 60 * 1000;

  public static final String TABLE_WHITELIST_CONFIG = "table.whitelist";
  private static final String TABLE_WHITELIST_DOC =
      "List of tables to include in copying. If specified, table.blacklist may not be set.";
  public static final String TABLE_WHITELIST_DEFAULT = "";

  public static final String TABLE_BLACKLIST_CONFIG = "table.blacklist";
  private static final String TABLE_BLACKLIST_DOC =
      "List of tables to exclude from copying. If specified, table.whitelist may not be set.";
  public static final String TABLE_BLACKLIST_DEFAULT = "";

  public static final String QUERY_CONFIG = "query";
  private static final String QUERY_DOC =
      "If specified, the query to perform to select new or updated rows. Use this setting if you "
      + "want to join tables, select subsets of columns in a table, or filter data. If used, this"
      + " connector will only copy data using this query -- whole-table copying will be disabled."
      + " Different query modes may still be used for incremental updates, but in order to "
      + "properly construct the incremental query, it must be possible to append a WHERE clause "
      + "to this query (i.e. no WHERE clauses may be used). If you use a WHERE clause, it must "
      + "handle incremental queries itself.";
  public static final String QUERY_DEFAULT = "";

  public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
  private static final String TOPIC_PREFIX_DOC =
      "Prefix to prepend to table names to generate the name of the Kafka topic to publish data "
      + "to, or in the case of a custom query, the full name of the topic to publish to.";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
        .define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC)
        .define(POLL_INTERVAL_MS_CONFIG, Type.INT, POLL_INTERVAL_MS_DEFAULT, Importance.HIGH,
                POLL_INTERVAL_MS_DOC)
        .define(BATCH_MAX_ROWS_CONFIG, Type.INT, BATCH_MAX_ROWS_DEFAULT, Importance.LOW,
                BATCH_MAX_ROWS_DOC)
        .define(MODE_CONFIG, Type.STRING, MODE_UNSPECIFIED,
                ConfigDef.ValidString.in(Arrays.asList(MODE_UNSPECIFIED, MODE_BULK, MODE_TIMESTAMP,
                                                       MODE_INCREMENTING,
                                                       MODE_TIMESTAMP_INCREMENTING)),
                Importance.HIGH, MODE_DOC)
        .define(INCREMENTING_COLUMN_NAME_CONFIG, Type.STRING, INCREMENTING_COLUMN_NAME_DEFAULT,
                Importance.MEDIUM, INCREMENTING_COLUMN_NAME_DOC)
        .define(TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, TIMESTAMP_COLUMN_NAME_DEFAULT,
                Importance.MEDIUM, TIMESTAMP_COLUMN_NAME_DOC)
        .define(TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, Type.LONG, TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT,
                Importance.LOW, TIMESTAMP_DELAY_INTERVAL_MS_DOC)
        .define(TABLE_POLL_INTERVAL_MS_CONFIG, Type.LONG, TABLE_POLL_INTERVAL_MS_DEFAULT,
                Importance.LOW, TABLE_POLL_INTERVAL_MS_DOC)
        .define(TABLE_WHITELIST_CONFIG, Type.LIST, TABLE_WHITELIST_DEFAULT,
                Importance.MEDIUM, TABLE_WHITELIST_DOC)
        .define(TABLE_BLACKLIST_CONFIG, Type.LIST, TABLE_BLACKLIST_DEFAULT,
                Importance.MEDIUM, TABLE_BLACKLIST_DOC)
        .define(QUERY_CONFIG, Type.STRING, QUERY_DEFAULT,
                Importance.MEDIUM, QUERY_DOC)
        .define(TOPIC_PREFIX_CONFIG, Type.STRING,
                Importance.HIGH, TOPIC_PREFIX_DOC);
  }

  static ConfigDef config = baseConfigDef();

  public JdbcSourceConnectorConfig(Map<String, String> props) {
    super(config, props);
    String mode = getString(JdbcSourceConnectorConfig.MODE_CONFIG);
    if (mode.equals(JdbcSourceConnectorConfig.MODE_UNSPECIFIED))
      throw new ConfigException("Query mode must be specified");
  }

  protected JdbcSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
    super(subclassConfigDef, props);
  }


  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
