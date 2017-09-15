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

import io.confluent.connect.jdbc.util.JdbcUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class JdbcSourceConnectorConfig extends AbstractConfig {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceConnectorConfig.class);

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC = "JDBC connection URL.";
  private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

  public static final String CONNECTION_USER_CONFIG = "connection.user";
  private static final String CONNECTION_USER_DOC = "JDBC connection user.";
  private static final String CONNECTION_USER_DISPLAY = "JDBC User";

  public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
  private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

  public static final String CONNECTION_ATTEMPTS_CONFIG = "connection.attempts";
  private static final String CONNECTION_ATTEMPTS_DOC
      = "Maximum number of attempts to retrieve a valid JDBC connection.";
  private static final String CONNECTION_ATTEMPTS_DISPLAY = "JDBC connection attempts";
  public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

  public static final String CONNECTION_BACKOFF_CONFIG = "connection.backoff.ms";
  private static final String CONNECTION_BACKOFF_DOC
      = "Backoff time in milliseconds between connection attempts.";
  private static final String CONNECTION_BACKOFF_DISPLAY
      = "JDBC connection backoff in milliseconds";
  public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

  public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
  private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
                                                     + "each table.";
  public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
  private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

  public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
  private static final String BATCH_MAX_ROWS_DOC =
      "Maximum number of rows to include in a single batch when polling for new data. This "
      + "setting can be used to limit the amount of data buffered internally in the connector.";
  public static final int BATCH_MAX_ROWS_DEFAULT = 100;
  private static final String BATCH_MAX_ROWS_DISPLAY = "Max Rows Per Batch";

  public static final String NUMERIC_PRECISION_MAPPING_CONFIG = "numeric.precision.mapping";
  private static final String NUMERIC_PRECISION_MAPPING_DOC =
          "Whether or not to attempt mapping NUMERIC values by precision to integral types";
  public static final boolean NUMERIC_PRECISION_MAPPING_DEFAULT = false;
  private static final String NUMERIC_PRECISION_MAPPING_DISPLAY = "Map Numeric Values By Precision";

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
  private static final String MODE_DISPLAY = "Table Loading Mode";

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
  private static final String INCREMENTING_COLUMN_NAME_DISPLAY = "Incrementing Column Name";

  public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
  private static final String TIMESTAMP_COLUMN_NAME_DOC =
      "The name of the timestamp column to use to detect new or modified rows. This column may "
      + "not be nullable.";
  public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";
  private static final String TIMESTAMP_COLUMN_NAME_DISPLAY = "Timestamp Column Name";

  public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
  private static final String TABLE_POLL_INTERVAL_MS_DOC =
      "Frequency in ms to poll for new or removed tables, which may result in updated task "
      + "configurations to start polling for data in added tables or stop polling for data in "
      + "removed tables.";
  public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 60 * 1000;
  private static final String TABLE_POLL_INTERVAL_MS_DISPLAY
      = "Metadata Change Monitoring Interval (ms)";

  public static final String TABLE_WHITELIST_CONFIG = "table.whitelist";
  private static final String TABLE_WHITELIST_DOC =
      "List of tables to include in copying. If specified, table.blacklist may not be set.";
  public static final String TABLE_WHITELIST_DEFAULT = "";
  private static final String TABLE_WHITELIST_DISPLAY = "Table Whitelist";

  public static final String TABLE_BLACKLIST_CONFIG = "table.blacklist";
  private static final String TABLE_BLACKLIST_DOC =
      "List of tables to exclude from copying. If specified, table.whitelist may not be set.";
  public static final String TABLE_BLACKLIST_DEFAULT = "";
  private static final String TABLE_BLACKLIST_DISPLAY = "Table Blacklist";

  public static final String SCHEMA_PATTERN_CONFIG = "schema.pattern";
  private static final String SCHEMA_PATTERN_DOC =
      "Schema pattern to fetch tables metadata from the database:\n"
      + "  * \"\" retrieves those without a schema,"
      + "  * null (default) means that the schema name should not be used to narrow the search, "
      + "all tables "
      + "metadata would be fetched, regardless their schema.";
  private static final String SCHEMA_PATTERN_DISPLAY = "Schema pattern";

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
  private static final String QUERY_DISPLAY = "Query";

  public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
  private static final String TOPIC_PREFIX_DOC =
      "Prefix to prepend to table names to generate the name of the Kafka topic to publish data "
      + "to, or in the case of a custom query, the full name of the topic to publish to.";
  private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";

  public static final String VALIDATE_NON_NULL_CONFIG = "validate.non.null";
  private static final String VALIDATE_NON_NULL_DOC =
      "By default, the JDBC connector will validate that all incrementing and timestamp tables "
      + "have NOT NULL set for the columns being used as their ID/timestamp. If the tables don't,"
      + " JDBC connector will fail to start. Setting this to false will disable these checks.";
  public static final boolean VALIDATE_NON_NULL_DEFAULT = true;
  private static final String VALIDATE_NON_NULL_DISPLAY = "Validate Non Null";

  public static final String TIMESTAMP_DELAY_INTERVAL_MS_CONFIG = "timestamp.delay.interval.ms";
  private static final String TIMESTAMP_DELAY_INTERVAL_MS_DOC =
      "How long to wait after a row with certain timestamp appears before we include it in the "
      + "result. You may choose to add some delay to allow transactions with earlier timestamp to"
      + " complete. The first execution will fetch all available records (i.e. starting at "
      + "timestamp 0) until current time minus the delay. Every following execution will get data"
      + " from the last time we fetched until current time minus the delay.";
  public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 0;
  private static final String TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY = "Delay Interval (ms)";

  public static final String DATABASE_GROUP = "Database";
  public static final String MODE_GROUP = "Mode";
  public static final String CONNECTOR_GROUP = "Connector";

  // We want the table recommender to only cache values for a short period of time so that the
  // blacklist and whitelist config properties can use a single query.
  private static final Recommender TABLE_RECOMMENDER = new CachingRecommender(
      new TableRecommender(),
      Time.SYSTEM,
      TimeUnit.SECONDS.toMillis(5)
  );
  private static final Recommender MODE_DEPENDENTS_RECOMMENDER =  new ModeDependentsRecommender();


  public static final String TABLE_TYPE_DEFAULT = "TABLE";
  public static final String TABLE_TYPE_CONFIG = "table.types";
  private static final String TABLE_TYPE_DOC =
      "By default, the JDBC connector will only detect tables with type TABLE from the source "
      + "Database. This config allows a command separated list of table types to extract. Options"
      + " include:\n"
      + "* TABLE\n"
      + "* VIEW\n"
      + "* SYSTEM TABLE\n"
      + "* GLOBAL TEMPORARY\n"
      + "* LOCAL TEMPORARY\n"
      + "* ALIAS\n"
      + "* SYNONYM\n"
      + "In most cases it only makes sense to have either TABLE or VIEW.";
  private static final String TABLE_TYPE_DISPLAY = "Table Types";

  public static ConfigDef baseConfigDef() {
    ConfigDef config = new ConfigDef();
    addDatabaseOptions(config);
    addModeOptions(config);
    addConnectorOptions(config);
    return config;
  }

  private static final void addDatabaseOptions(ConfigDef config) {
    config.define(
        CONNECTION_URL_CONFIG,
        Type.STRING,
        Importance.HIGH,
        CONNECTION_URL_DOC,
        DATABASE_GROUP,
        1,
        Width.LONG,
        CONNECTION_URL_DISPLAY,
        Arrays.asList(TABLE_WHITELIST_CONFIG, TABLE_BLACKLIST_CONFIG)
    ).define(
        CONNECTION_USER_CONFIG,
        Type.STRING,
        null,
        Importance.HIGH,
        CONNECTION_USER_DOC,
        DATABASE_GROUP,
        2,
        Width.LONG,
        CONNECTION_USER_DISPLAY
    ).define(
        CONNECTION_PASSWORD_CONFIG,
        Type.PASSWORD,
        null,
        Importance.HIGH,
        CONNECTION_PASSWORD_DOC,
        DATABASE_GROUP,
        3,
        Width.SHORT,
        CONNECTION_PASSWORD_DISPLAY
    ).define(
        CONNECTION_ATTEMPTS_CONFIG,
        Type.INT,
        CONNECTION_ATTEMPTS_DEFAULT,
        Importance.LOW,
        CONNECTION_ATTEMPTS_DOC,
        DATABASE_GROUP,
        4,
        Width.SHORT,
        CONNECTION_ATTEMPTS_DISPLAY
    ).define(
        CONNECTION_BACKOFF_CONFIG,
        Type.LONG,
        CONNECTION_BACKOFF_DEFAULT,
        Importance.LOW,
        CONNECTION_BACKOFF_DOC,
        DATABASE_GROUP,
        5,
        Width.SHORT,
        CONNECTION_BACKOFF_DISPLAY
    ).define(
        TABLE_WHITELIST_CONFIG,
        Type.LIST,
        TABLE_WHITELIST_DEFAULT,
        Importance.MEDIUM,
        TABLE_WHITELIST_DOC,
        DATABASE_GROUP,
        4,
        Width.LONG,
        TABLE_WHITELIST_DISPLAY,
        TABLE_RECOMMENDER
    ).define(
        TABLE_BLACKLIST_CONFIG,
        Type.LIST,
        TABLE_BLACKLIST_DEFAULT,
        Importance.MEDIUM,
        TABLE_BLACKLIST_DOC,
        DATABASE_GROUP,
        5,
        Width.LONG,
        TABLE_BLACKLIST_DISPLAY,
        TABLE_RECOMMENDER
    ).define(
        SCHEMA_PATTERN_CONFIG,
        Type.STRING,
        null,
        Importance.MEDIUM,
        SCHEMA_PATTERN_DOC,
        DATABASE_GROUP,
        6,
        Width.SHORT,
        SCHEMA_PATTERN_DISPLAY
    ).define(
        NUMERIC_PRECISION_MAPPING_CONFIG,
        Type.BOOLEAN,
        NUMERIC_PRECISION_MAPPING_DEFAULT,
        Importance.LOW,
        NUMERIC_PRECISION_MAPPING_DOC,
        DATABASE_GROUP,
        4,
        Width.SHORT,
        NUMERIC_PRECISION_MAPPING_DISPLAY);
  }

  private static final void addModeOptions(ConfigDef config) {
    config.define(
        MODE_CONFIG,
        Type.STRING,
        MODE_UNSPECIFIED,
        ConfigDef.ValidString.in(
            MODE_UNSPECIFIED,
            MODE_BULK,
            MODE_TIMESTAMP,
            MODE_INCREMENTING,
            MODE_TIMESTAMP_INCREMENTING
        ),
        Importance.HIGH,
        MODE_DOC,
        MODE_GROUP,
        1,
        Width.MEDIUM,
        MODE_DISPLAY,
        Arrays.asList(
            INCREMENTING_COLUMN_NAME_CONFIG,
            TIMESTAMP_COLUMN_NAME_CONFIG,
            VALIDATE_NON_NULL_CONFIG
        )
    ).define(
        INCREMENTING_COLUMN_NAME_CONFIG,
        Type.STRING,
        INCREMENTING_COLUMN_NAME_DEFAULT,
        Importance.MEDIUM,
        INCREMENTING_COLUMN_NAME_DOC,
        MODE_GROUP,
        2,
        Width.MEDIUM,
        INCREMENTING_COLUMN_NAME_DISPLAY,
        MODE_DEPENDENTS_RECOMMENDER
    ).define(
        TIMESTAMP_COLUMN_NAME_CONFIG,
        Type.STRING,
        TIMESTAMP_COLUMN_NAME_DEFAULT,
        Importance.MEDIUM,
        TIMESTAMP_COLUMN_NAME_DOC,
        MODE_GROUP,
        3,
        Width.MEDIUM,
        TIMESTAMP_COLUMN_NAME_DISPLAY,
        MODE_DEPENDENTS_RECOMMENDER
    ).define(
        VALIDATE_NON_NULL_CONFIG,
        Type.BOOLEAN,
        VALIDATE_NON_NULL_DEFAULT,
        Importance.LOW,
        VALIDATE_NON_NULL_DOC,
        MODE_GROUP,
        4,
        Width.SHORT,
        VALIDATE_NON_NULL_DISPLAY,
        MODE_DEPENDENTS_RECOMMENDER
    ).define(
        QUERY_CONFIG,
        Type.STRING,
        QUERY_DEFAULT,
        Importance.MEDIUM,
        QUERY_DOC,
        MODE_GROUP,
        5,
        Width.SHORT,
        QUERY_DISPLAY);
  }

  private static final void addConnectorOptions(ConfigDef config) {
    config.define(
        TABLE_TYPE_CONFIG,
        Type.LIST,
        TABLE_TYPE_DEFAULT,
        Importance.LOW,
        TABLE_TYPE_DOC,
        CONNECTOR_GROUP,
        4,
        Width.MEDIUM,
        TABLE_TYPE_DISPLAY
    ).define(
        POLL_INTERVAL_MS_CONFIG,
        Type.INT,
        POLL_INTERVAL_MS_DEFAULT,
        Importance.HIGH,
        POLL_INTERVAL_MS_DOC,
        CONNECTOR_GROUP,
        1,
        Width.SHORT,
        POLL_INTERVAL_MS_DISPLAY
    ).define(
        BATCH_MAX_ROWS_CONFIG,
        Type.INT,
        BATCH_MAX_ROWS_DEFAULT,
        Importance.LOW,
        BATCH_MAX_ROWS_DOC,
        CONNECTOR_GROUP,
        2,
        Width.SHORT,
        BATCH_MAX_ROWS_DISPLAY
    ).define(
        TABLE_POLL_INTERVAL_MS_CONFIG,
        Type.LONG,
        TABLE_POLL_INTERVAL_MS_DEFAULT,
        Importance.LOW,
        TABLE_POLL_INTERVAL_MS_DOC,
        CONNECTOR_GROUP,
        3,
        Width.SHORT,
        TABLE_POLL_INTERVAL_MS_DISPLAY
    ).define(
        TOPIC_PREFIX_CONFIG,
        Type.STRING,
        Importance.HIGH,
        TOPIC_PREFIX_DOC,
        CONNECTOR_GROUP,
        4,
        Width.MEDIUM,
        TOPIC_PREFIX_DISPLAY
    ).define(
        TIMESTAMP_DELAY_INTERVAL_MS_CONFIG,
        Type.LONG,
        TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT,
        Importance.HIGH,
        TIMESTAMP_DELAY_INTERVAL_MS_DOC,
        CONNECTOR_GROUP,
        5,
        Width.MEDIUM,
        TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public JdbcSourceConnectorConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    String mode = getString(JdbcSourceConnectorConfig.MODE_CONFIG);
    if (mode.equals(JdbcSourceConnectorConfig.MODE_UNSPECIFIED)) {
      throw new ConfigException("Query mode must be specified");
    }
  }

  private static class TableRecommender implements Recommender {

    @Override
    public List<Object> validValues(String name, Map<String, Object> config) {
      String dbUrl = (String) config.get(CONNECTION_URL_CONFIG);
      String dbUser = (String) config.get(CONNECTION_USER_CONFIG);
      Password dbPassword = (Password) config.get(CONNECTION_PASSWORD_CONFIG);
      String schemaPattern = (String) config.get(JdbcSourceTaskConfig.SCHEMA_PATTERN_CONFIG);
      Set<String> tableTypes = new HashSet<>(
          (List<String>) config.get(JdbcSourceTaskConfig.TABLE_TYPE_CONFIG)
      );
      if (dbUrl == null) {
        throw new ConfigException(CONNECTION_URL_CONFIG + " cannot be null.");
      }
      String dbPasswordStr = dbPassword == null ? null : dbPassword.value();
      try (Connection db = DriverManager.getConnection(dbUrl, dbUser, dbPasswordStr)) {
        return new LinkedList<Object>(JdbcUtils.getTables(db, schemaPattern, tableTypes));
      } catch (SQLException e) {
        throw new ConfigException("Couldn't open connection to " + dbUrl, e);
      }
    }

    @Override
    public boolean visible(String name, Map<String, Object> config) {
      return true;
    }
  }

  /**
   * A recommender that caches values returned by a delegate, where the cache remains valid for a
   * specified duration and as long as the configuration remains unchanged.
   */
  static class CachingRecommender implements Recommender {

    private final Time time;
    private final long cacheDurationInMillis;
    private final AtomicReference<CachedRecommenderValues> cachedValues
        = new AtomicReference<>(new CachedRecommenderValues());
    private final Recommender delegate;

    public CachingRecommender(Recommender delegate, Time time, long cacheDurationInMillis) {
      this.delegate = delegate;
      this.time = time;
      this.cacheDurationInMillis = cacheDurationInMillis;
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> config) {
      List<Object> results = cachedValues.get().cachedValue(config, time.milliseconds());
      if (results != null) {
        LOG.debug("Returning cached table names: {}", results);
        return results;
      }
      LOG.trace("Fetching table names");
      results = delegate.validValues(name, config);
      LOG.debug("Caching table names: {}", results);
      long expireTime = time.milliseconds() + cacheDurationInMillis;
      cachedValues.set(new CachedRecommenderValues(config, results, expireTime));
      return results;
    }

    @Override
    public boolean visible(String name, Map<String, Object> config) {
      return true;
    }
  }

  static class CachedRecommenderValues {
    private final Map<String, Object> lastConfig;
    private final List<Object> results;
    private final long expiryTimeInMillis;

    public CachedRecommenderValues() {
      this(null, null, 0L);
    }

    public CachedRecommenderValues(
        Map<String, Object> lastConfig,
        List<Object> results, long expiryTimeInMillis) {
      this.lastConfig = lastConfig;
      this.results = results;
      this.expiryTimeInMillis = expiryTimeInMillis;
    }

    public List<Object> cachedValue(Map<String, Object> config, long currentTimeInMillis) {
      if (currentTimeInMillis < expiryTimeInMillis
          && lastConfig != null && lastConfig.equals(config)) {
        return results;
      }
      return null;
    }
  }

  private static class ModeDependentsRecommender implements Recommender {

    @Override
    public List<Object> validValues(String name, Map<String, Object> config) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> config) {
      String mode = (String) config.get(MODE_CONFIG);
      switch (mode) {
        case MODE_BULK:
          return false;
        case MODE_TIMESTAMP:
          return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG) || name.equals(VALIDATE_NON_NULL_CONFIG);
        case MODE_INCREMENTING:
          return name.equals(INCREMENTING_COLUMN_NAME_CONFIG)
                 || name.equals(VALIDATE_NON_NULL_CONFIG);
        case MODE_TIMESTAMP_INCREMENTING:
          return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG)
                 || name.equals(INCREMENTING_COLUMN_NAME_CONFIG)
                 || name.equals(VALIDATE_NON_NULL_CONFIG);
        case MODE_UNSPECIFIED:
          throw new ConfigException("Query mode must be specified");
        default:
          throw new ConfigException("Invalid mode: " + mode);
      }
    }
  }

  protected JdbcSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
    super(subclassConfigDef, props);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }
}
