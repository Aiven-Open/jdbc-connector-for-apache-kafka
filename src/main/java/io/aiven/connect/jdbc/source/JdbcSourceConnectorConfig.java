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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialects;
import io.aiven.connect.jdbc.util.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSourceConnectorConfig extends JdbcConfig {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceConnectorConfig.class);

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
        "Whether or not to attempt mapping NUMERIC values by precision to integral types. This "
            + "option is now deprecated. A future version may remove it completely. Please use "
            + "``numeric.mapping`` instead.";
    public static final boolean NUMERIC_PRECISION_MAPPING_DEFAULT = false;
    private static final String NUMERIC_PRECISION_MAPPING_DISPLAY = "Map Numeric Values By "
        + "Precision (deprecated)";

    public static final String NUMERIC_MAPPING_CONFIG = "numeric.mapping";
    private static final String NUMERIC_MAPPING_DOC =
        "Map NUMERIC values by precision and optionally scale to integral or decimal types. Use "
            + "``none`` if all NUMERIC columns are to be represented by Connect's DECIMAL logical "
            + "type. Use ``best_fit`` if NUMERIC columns should be cast to Connect's INT8, INT16, "
            + "INT32, INT64, or FLOAT64 based upon the column's precision and scale. Or use "
            + "``precision_only`` to map NUMERIC columns based only on the column's precision "
            + "assuming that column's scale is 0. The ``none`` option is the default, but may lead "
            + "to serialization issues with Avro since Connect's DECIMAL type is mapped to its "
            + "binary representation, and ``best_fit`` will often be preferred since it maps to the"
            + " most appropriate primitive type.";

    public static final String NUMERIC_MAPPING_DEFAULT = null;
    private static final String NUMERIC_MAPPING_DISPLAY = "Map Numeric Values, Integral "
        + "or Decimal, By Precision and Scale";

    private static final EnumRecommender NUMERIC_MAPPING_RECOMMENDER =
        EnumRecommender.in(NumericMapping.values());

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

    public static final String INCREMENTING_INITIAL_VALUE_CONFIG = "incrementing.initial";
    private static final String INCREMENTING_INITIAL_VALUE_DOC =
            "For the incrementing column, consider only the rows that have the value greater than this."
            + " Use this if you need to pick up rows with negative or zero value, "
            + " or if you want to skip rows.";
    public static final long INCREMENTING_INITIAL_VALUE_DEFAULT = -1;
    private static final String INCREMENTING_INITIAL_VALUE_DISPLAY = "Incrementing Column Initial Value";

    public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
    private static final String TIMESTAMP_COLUMN_NAME_DOC =
        "Comma separated list of one or more timestamp columns to detect new or modified rows using "
            + "the COALESCE SQL function. Rows whose first non-null timestamp value is greater than the "
            + "largest previous timestamp value seen will be discovered with each poll. At least one "
            + "column should not be nullable.";
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
        "Schema pattern to fetch table metadata from the database:\n"
            + "  * \"\" retrieves those without a schema,\n"
            + "  * null (default) means that the schema name should not be used to narrow the search,"
            + " so that all table metadata would be fetched, regardless of their schema.";
    private static final String SCHEMA_PATTERN_DISPLAY = "Schema pattern";
    public static final String SCHEMA_PATTERN_DEFAULT = null;

    public static final String CATALOG_PATTERN_CONFIG = "catalog.pattern";
    private static final String CATALOG_PATTERN_DOC =
        "Catalog pattern to fetch table metadata from the database:\n"
            + "  * \"\" retrieves those without a catalog,\n"
            + "  * null (default) means that the catalog name should not be used to narrow the search"
            + " so that all table metadata would be fetched, regardless of their catalog.";
    private static final String CATALOG_PATTERN_DISPLAY = "Catalog pattern";
    public static final String CATALOG_PATTERN_DEFAULT = null;

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
            + "timestamp greater than 0) until current time minus the delay. Every following execution will get data"
            + " from the last time we fetched until current time minus the delay.";
    public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 0;
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY = "Delay Interval (ms)";

    public static final String TIMESTAMP_INITIAL_MS_CONFIG = "timestamp.initial.ms";
    private static final String TIMESTAMP_INITIAL_MS_DOC =
            "The initial value of timestamp when selecting records. "
                + "The records having timestamp greater than the value are included in the result."
                + " Defaults to 0.";
    public static final long TIMESTAMP_INITIAL_MS_DEFAULT = 0;
    private static final String TIMESTAMP_INITIAL_MS_DISPLAY = "Initial timestamp (ms since epoch, can be negative)";

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
    private static final Recommender MODE_DEPENDENTS_RECOMMENDER = new ModeDependentsRecommender();

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


    public static final String TABLE_NAMES_QUALIFY_CONFIG = "table.names.qualify";
    private static final String TABLE_NAMES_QUALIFY_DOC =
            "Whether to use fully-qualified table names when querying the database. If disabled, "
                    + "queries will be performed with unqualified table names. This may be useful if the "
                    + "database has been configured with a search path to automatically direct unqualified "
                    + "queries to the correct table when there are multiple tables available with the same "
                    + "unqualified name";
    public static final boolean TABLE_NAMES_QUALIFY_DEFAULT = true;
    private static final String TABLE_NAMES_QUALIFY_DISPLAY = "Qualify table names";

    public static ConfigDef baseConfigDef() {
        final ConfigDef config = new ConfigDef();
        addDatabaseOptions(config);
        addModeOptions(config);
        addConnectorOptions(config);
        return config;
    }

    private static final void addDatabaseOptions(final ConfigDef config) {
        int orderInGroup = 0;
        defineConnectionUrl(config, ++orderInGroup,
            Arrays.asList(TABLE_WHITELIST_CONFIG, TABLE_BLACKLIST_CONFIG));
        defineConnectionUser(config, ++orderInGroup);
        defineConnectionPassword(config, ++orderInGroup);

        config.define(
            CONNECTION_ATTEMPTS_CONFIG,
            Type.INT,
            CONNECTION_ATTEMPTS_DEFAULT,
            Importance.LOW,
            CONNECTION_ATTEMPTS_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            CONNECTION_ATTEMPTS_DISPLAY
        ).define(
            CONNECTION_BACKOFF_CONFIG,
            Type.LONG,
            CONNECTION_BACKOFF_DEFAULT,
            Importance.LOW,
            CONNECTION_BACKOFF_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            CONNECTION_BACKOFF_DISPLAY
        ).define(
            TABLE_WHITELIST_CONFIG,
            Type.LIST,
            TABLE_WHITELIST_DEFAULT,
            Importance.MEDIUM,
            TABLE_WHITELIST_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
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
            ++orderInGroup,
            Width.LONG,
            TABLE_BLACKLIST_DISPLAY,
            TABLE_RECOMMENDER
        ).define(
            CATALOG_PATTERN_CONFIG,
            Type.STRING,
            CATALOG_PATTERN_DEFAULT,
            Importance.MEDIUM,
            CATALOG_PATTERN_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            CATALOG_PATTERN_DISPLAY
        ).define(
            SCHEMA_PATTERN_CONFIG,
            Type.STRING,
            SCHEMA_PATTERN_DEFAULT,
            Importance.MEDIUM,
            SCHEMA_PATTERN_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            SCHEMA_PATTERN_DISPLAY
        ).define(
            NUMERIC_PRECISION_MAPPING_CONFIG,
            Type.BOOLEAN,
            NUMERIC_PRECISION_MAPPING_DEFAULT,
            Importance.LOW,
            NUMERIC_PRECISION_MAPPING_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            NUMERIC_PRECISION_MAPPING_DISPLAY
        ).define(
            NUMERIC_MAPPING_CONFIG,
            Type.STRING,
            NUMERIC_MAPPING_DEFAULT,
            NUMERIC_MAPPING_RECOMMENDER,
            Importance.LOW,
            NUMERIC_MAPPING_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            NUMERIC_MAPPING_DISPLAY,
            NUMERIC_MAPPING_RECOMMENDER
        ).define(
                TABLE_NAMES_QUALIFY_CONFIG,
            Type.BOOLEAN,
                TABLE_NAMES_QUALIFY_DEFAULT,
            Importance.LOW,
                TABLE_NAMES_QUALIFY_DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
                TABLE_NAMES_QUALIFY_DISPLAY
        );

        defineDbTimezone(config, ++orderInGroup);
        defineDialectName(config, ++orderInGroup);
        defineSqlQuoteIdentifiers(config, ++orderInGroup);
    }

    private static final void addModeOptions(final ConfigDef config) {
        int orderInGroup = 0;
        config.define(
            MODE_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.ValidString.in(
                MODE_BULK,
                MODE_TIMESTAMP,
                MODE_INCREMENTING,
                MODE_TIMESTAMP_INCREMENTING
            ),
            Importance.HIGH,
            MODE_DOC,
            MODE_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            MODE_DISPLAY,
            Arrays.asList(
                INCREMENTING_COLUMN_NAME_CONFIG,
                TIMESTAMP_COLUMN_NAME_CONFIG,
                VALIDATE_NON_NULL_CONFIG,
                TIMESTAMP_INITIAL_MS_CONFIG,
                INCREMENTING_INITIAL_VALUE_CONFIG
            )
        ).define(
            INCREMENTING_COLUMN_NAME_CONFIG,
            Type.STRING,
            INCREMENTING_COLUMN_NAME_DEFAULT,
            Importance.MEDIUM,
            INCREMENTING_COLUMN_NAME_DOC,
            MODE_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            INCREMENTING_COLUMN_NAME_DISPLAY,
            MODE_DEPENDENTS_RECOMMENDER
        ).define(
            TIMESTAMP_COLUMN_NAME_CONFIG,
            Type.LIST,
            TIMESTAMP_COLUMN_NAME_DEFAULT,
            Importance.MEDIUM,
            TIMESTAMP_COLUMN_NAME_DOC,
            MODE_GROUP,
            ++orderInGroup,
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
            ++orderInGroup,
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
            ++orderInGroup,
            Width.SHORT,
            QUERY_DISPLAY
        ).define(
            TIMESTAMP_INITIAL_MS_CONFIG,
            Type.LONG,
            TIMESTAMP_INITIAL_MS_DEFAULT,
            Importance.MEDIUM,
            TIMESTAMP_INITIAL_MS_DOC,
            MODE_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            TIMESTAMP_INITIAL_MS_DISPLAY
        ).define(
            INCREMENTING_INITIAL_VALUE_CONFIG,
            Type.LONG,
            INCREMENTING_INITIAL_VALUE_DEFAULT,
            Importance.MEDIUM,
            INCREMENTING_INITIAL_VALUE_DOC,
            MODE_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            INCREMENTING_INITIAL_VALUE_DISPLAY
        );
    }

    private static final void addConnectorOptions(final ConfigDef config) {
        int orderInGroup = 0;
        config.define(
            TABLE_TYPE_CONFIG,
            Type.LIST,
            TABLE_TYPE_DEFAULT,
            Importance.LOW,
            TABLE_TYPE_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            TABLE_TYPE_DISPLAY
        ).define(
            POLL_INTERVAL_MS_CONFIG,
            Type.INT,
            POLL_INTERVAL_MS_DEFAULT,
            Importance.HIGH,
            POLL_INTERVAL_MS_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.SHORT,
            POLL_INTERVAL_MS_DISPLAY
        ).define(
            BATCH_MAX_ROWS_CONFIG,
            Type.INT,
            BATCH_MAX_ROWS_DEFAULT,
            Importance.LOW,
            BATCH_MAX_ROWS_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.SHORT,
            BATCH_MAX_ROWS_DISPLAY
        ).define(
            TABLE_POLL_INTERVAL_MS_CONFIG,
            Type.LONG,
            TABLE_POLL_INTERVAL_MS_DEFAULT,
            Importance.LOW,
            TABLE_POLL_INTERVAL_MS_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.SHORT,
            TABLE_POLL_INTERVAL_MS_DISPLAY
        ).define(
            TOPIC_PREFIX_CONFIG,
            Type.STRING,
            Importance.HIGH,
            TOPIC_PREFIX_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            TOPIC_PREFIX_DISPLAY
        ).define(
            TIMESTAMP_DELAY_INTERVAL_MS_CONFIG,
            Type.LONG,
            TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT,
            Importance.HIGH,
            TIMESTAMP_DELAY_INTERVAL_MS_DOC,
            CONNECTOR_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY
        );
    }

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public JdbcSourceConnectorConfig(final Map<String, ?> props) {
        super(CONFIG_DEF, props);
        if (props.containsKey(NUMERIC_PRECISION_MAPPING_CONFIG)) {
            LOG.warn("Option {} is deprecated and may be removed in future versions. "
                    + "Please use {} instead (see the documentation).",
                NUMERIC_PRECISION_MAPPING_CONFIG,
                NUMERIC_MAPPING_CONFIG);
        }
    }

    private static class TableRecommender implements Recommender {

        @SuppressWarnings("unchecked")
        @Override
        public List<Object> validValues(final String name, final Map<String, Object> config) {
            try {
                final String dbUrl = (String) config.get(CONNECTION_URL_CONFIG);
                if (dbUrl == null) {
                    throw new ConfigException(CONNECTION_URL_CONFIG + " cannot be null.");
                }
                // Create the dialect to get the tables ...
                final JdbcConfig jdbcConfig = new JdbcConfig(CONFIG_DEF, config);
                final DatabaseDialect dialect = DatabaseDialects.findBestFor(dbUrl, jdbcConfig);
                try (final Connection db = dialect.getConnection()) {
                    final List<Object> result = new LinkedList<>();
                    for (final TableId id : dialect.tableIds(db)) {
                        // Just add the unqualified table name
                        result.add(id.tableName());
                    }
                    return result;
                } catch (final SQLException e) {
                    throw new ConfigException("Couldn't open connection to " + dbUrl, e);
                }
            } catch (final Exception ex) {
                throw new ConfigException("Can't get recommended tables: " + ex.getMessage());
            }
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> config) {
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

        public CachingRecommender(final Recommender delegate, final Time time, final long cacheDurationInMillis) {
            this.delegate = delegate;
            this.time = time;
            this.cacheDurationInMillis = cacheDurationInMillis;
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> config) {
            List<Object> results = cachedValues.get().cachedValue(config, time.milliseconds());
            if (results != null) {
                LOG.debug("Returning cached table names: {}", results);
                return results;
            }
            LOG.trace("Fetching table names");
            results = delegate.validValues(name, config);
            LOG.debug("Caching table names: {}", results);
            final long expireTime = time.milliseconds() + cacheDurationInMillis;
            cachedValues.set(new CachedRecommenderValues(config, results, expireTime));
            return results;
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> config) {
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
            final Map<String, Object> lastConfig,
            final List<Object> results, final long expiryTimeInMillis) {
            this.lastConfig = lastConfig;
            this.results = results;
            this.expiryTimeInMillis = expiryTimeInMillis;
        }

        public List<Object> cachedValue(final Map<String, Object> config, final long currentTimeInMillis) {
            if (currentTimeInMillis < expiryTimeInMillis
                && lastConfig != null && lastConfig.equals(config)) {
                return results;
            }
            return null;
        }
    }

    private static class ModeDependentsRecommender implements Recommender {

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> config) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> config) {
            final String mode = (String) config.get(MODE_CONFIG);

            if (mode == null) {
                return true;
            }

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
                default:
                    throw new ConfigException("Invalid mode: " + mode);
            }
        }
    }

    public enum NumericMapping {
        NONE,
        PRECISION_ONLY,
        BEST_FIT;

        private static final Map<String, NumericMapping> REVERSE = new HashMap<>(values().length);

        static {
            for (final NumericMapping val : values()) {
                REVERSE.put(val.name().toLowerCase(Locale.ROOT), val);
            }
        }

        public static NumericMapping get(final String prop) {
            // not adding a check for null value because the recommender/validator should catch those.
            return REVERSE.get(prop.toLowerCase(Locale.ROOT));
        }

        public static NumericMapping get(final JdbcSourceConnectorConfig config) {
            final String newMappingConfig = config.getString(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG);
            // We use 'null' as default to be able to check the old config if the new one is unset.
            if (newMappingConfig != null) {
                return get(config.getString(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG));
            }
            if (config.getBoolean(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG)) {
                return NumericMapping.PRECISION_ONLY;
            }
            return NumericMapping.NONE;
        }
    }

    //Porting from JdbcSinkConfig and extending to implement Recommender interface too.
    //TODO: Should factor out to common class.
    private static class EnumRecommender implements ConfigDef.Validator, ConfigDef.Recommender {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumRecommender(final List<String> canonicalValues, final Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        @SafeVarargs
        public static <E> EnumRecommender in(final E... enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length * 2);
            for (final E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toUpperCase(Locale.ROOT));
                validValues.add(e.toString().toLowerCase(Locale.ROOT));
            }
            return new EnumRecommender(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(final String key, final Object value) {
            // calling toString on itself because IDE complains if the Object is passed.
            if (value != null && !validValues.contains(value.toString())) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> connectorConfigs) {
            return new ArrayList<Object>(canonicalValues);
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    protected JdbcSourceConnectorConfig(final ConfigDef subclassConfigDef, final Map<String, String> props) {
        super(subclassConfigDef, props);
    }

    public NumericMapping numericMapping() {
        return NumericMapping.get(this);
    }

    public String getMode() {
        return getString(MODE_CONFIG);
    }

    public static void main(final String[] args) {
        System.out.println("===========================================");
        System.out.println("JDBC Source connector Configuration Options");
        System.out.println("===========================================");
        System.out.println();
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
