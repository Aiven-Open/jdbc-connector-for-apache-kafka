/*
 * Copyright 2024 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
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

package io.aiven.connect.jdbc.sink;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkConfig extends JdbcConfig {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkConfig.class);

    public enum InsertMode {
        INSERT,
        MULTI,
        UPSERT,
        UPDATE;
    }

    public enum PrimaryKeyMode {
        NONE,
        KAFKA,
        RECORD_KEY,
        RECORD_VALUE;
    }

    public static final List<String> DEFAULT_KAFKA_PK_NAMES = Collections.unmodifiableList(
        Arrays.asList(
            "__connect_topic",
            "__connect_partition",
            "__connect_offset"
        )
    );

    public static final String TABLE_NAME_FORMAT = "table.name.format";
    public static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
    private static final String TABLE_NAME_FORMAT_DOC =
        "A format string for the destination table name, which may contain '${topic}' as a "
            + "placeholder for the originating topic name.\n"
            + "For example, ``kafka_${topic}`` for the topic 'orders' will map to the table name "
            + "'kafka_orders'.";
    private static final String TABLE_NAME_FORMAT_DISPLAY = "Table Name Format";

    public static final String TABLE_NAME_NORMALIZE = "table.name.normalize";
    public static final boolean TABLE_NAME_NORMALIZE_DEFAULT = false;
    private static final String TABLE_NAME_NORMALIZE_DOC =
            "Whether or not to normalize destination table names for topics. "
                    + "When set to ``true``, the alphanumeric characters (``a-z A-Z 0-9``) and ``_`` "
                    + "remain as is, others (like ``.``) are replaced with ``_``.";
    private static final String TABLE_NAME_NORMALIZE_DISPLAY = "Table Name Normalize";

    public static final String TOPICS_TO_TABLES_MAPPING = "topics.to.tables.mapping";
    private static final String TOPICS_TO_TABLES_MAPPING_DOC =
            "Kafka topics to database tables mapping. "
                    + "Comma-separated list of topic to table mapping in the format: topic_name:table_name. "
                    + "If the destination table found in the mapping, "
                    + "it would override generated one defined in " + TABLE_NAME_FORMAT + ".";
    private static final String TOPICS_TO_TABLES_MAPPING_DISPLAY = "Topics To Tables Mapping";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 10;
    private static final String MAX_RETRIES_DOC =
        "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
    private static final String RETRY_BACKOFF_MS_DOC =
        "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT = 3000;
    private static final String BATCH_SIZE_DOC =
        "Specifies how many records to attempt to batch together for insertion into the destination"
            + " table, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";

    public static final String AUTO_CREATE = "auto.create";
    private static final String AUTO_CREATE_DEFAULT = "false";
    private static final String AUTO_CREATE_DOC =
        "Whether to automatically create the destination table based on record schema if it is "
            + "found to be missing by issuing ``CREATE``.";
    private static final String AUTO_CREATE_DISPLAY = "Auto-Create";

    public static final String AUTO_EVOLVE = "auto.evolve";
    private static final String AUTO_EVOLVE_DEFAULT = "false";
    private static final String AUTO_EVOLVE_DOC =
        "Whether to automatically add columns in the table schema when found to be missing relative "
            + "to the record schema by issuing ``ALTER``.";
    private static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

    public static final String INSERT_MODE = "insert.mode";
    private static final String INSERT_MODE_DEFAULT = "insert";
    private static final String INSERT_MODE_DOC =
        "The insertion mode to use. Supported modes are:\n"
            + "``insert``\n"
            + "    Use standard SQL ``INSERT`` statements.\n"
            + "``multi``\n"
            + "    Use multi-row ``INSERT`` statements.\n"
            + "``upsert``\n"
            + "    Use the appropriate upsert semantics for the target database if it is supported by "
            + "the connector, e.g. ``INSERT .. ON CONFLICT .. DO UPDATE SET ..``.\n"
            + "``update``\n"
            + "    Use the appropriate update semantics for the target database if it is supported by "
            + "the connector, e.g. ``UPDATE``.";
    private static final String INSERT_MODE_DISPLAY = "Insert Mode";

    public static final String PK_FIELDS = "pk.fields";
    private static final String PK_FIELDS_DEFAULT = "";
    private static final String PK_FIELDS_DOC =
        "List of comma-separated primary key field names. The runtime interpretation of this config"
            + " depends on the ``pk.mode``:\n"
            + "``none``\n"
            + "    Ignored as no fields are used as primary key in this mode.\n"
            + "``kafka``\n"
            + "    Must be a trio representing the Kafka coordinates (the topic, partition, and offset), defaults to ``"
            + StringUtils.join(DEFAULT_KAFKA_PK_NAMES, ",") + "`` if empty.\n"
            + "``record_key``\n"
            + "    If empty, all fields from the key struct will be used, otherwise used to extract the"
            + " desired fields - for primitive key only a single field name must be configured.\n"
            + "``record_value``\n"
            + "    If empty, all fields from the value struct will be used, otherwise used to extract "
            + "the desired fields.";
    private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

    public static final String PK_MODE = "pk.mode";
    private static final String PK_MODE_DEFAULT = "none";
    private static final String PK_MODE_DOC =
        "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
            + "Supported modes are:\n"
            + "``none``\n"
            + "    No keys utilized.\n"
            + "``kafka``\n"
            + "    Kafka coordinates (the topic, partition, and offset) are used as the PK.\n"
            + "``record_key``\n"
            + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
            + "``record_value``\n"
            + "    Field(s) from the record value are used, which must be a struct.";
    private static final String PK_MODE_DISPLAY = "Primary Key Mode";

    public static final String FIELDS_WHITELIST = "fields.whitelist";
    private static final String FIELDS_WHITELIST_DEFAULT = "";
    private static final String FIELDS_WHITELIST_DOC =
        "List of comma-separated record value field names. If empty, all fields from the record "
            + "value are utilized, otherwise used to filter to the desired fields.\n"
            + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
            + "(s) form the primary key columns in the destination database,"
            + " while this configuration is applicable for the other columns.";
    private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";

    public static final String DELETE_ENABLED = "delete.enabled";
    private static final String DELETE_ENABLED_DEFAULT = "false";
    private static final String DELETE_ENABLED_DOC =
            "Whether to enable the deletion of rows in the target table on tombstone messages";
    private static final String DELETE_ENABLED_DISPLAY = "Delete enabled";

    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

    private static final String WRITES_GROUP = "Writes";
    private static final String DATAMAPPING_GROUP = "Data Mapping";
    private static final String DDL_GROUP = "DDL Support";
    private static final String RETRIES_GROUP = "Retries";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    static {
        // Database
        int orderInGroup = 0;
        defineConnectionUrl(CONFIG_DEF, ++orderInGroup, Collections.emptyList());
        defineConnectionUser(CONFIG_DEF, ++orderInGroup);
        defineConnectionPassword(CONFIG_DEF, ++orderInGroup);
        defineDbTimezone(CONFIG_DEF, ++orderInGroup);
        defineDialectName(CONFIG_DEF, ++orderInGroup);
        defineSqlQuoteIdentifiers(CONFIG_DEF, ++orderInGroup);

        // Writes
        CONFIG_DEF
            .define(
                INSERT_MODE,
                ConfigDef.Type.STRING,
                INSERT_MODE_DEFAULT,
                EnumValidator.in(InsertMode.values()),
                ConfigDef.Importance.HIGH,
                INSERT_MODE_DOC,
                WRITES_GROUP,
                1,
                ConfigDef.Width.MEDIUM,
                INSERT_MODE_DISPLAY
            )
            .define(
                BATCH_SIZE,
                ConfigDef.Type.INT,
                BATCH_SIZE_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                BATCH_SIZE_DOC, WRITES_GROUP,
                2,
                ConfigDef.Width.SHORT,
                BATCH_SIZE_DISPLAY)
            .define(
                // Delete can only be enabled with delete.enabled=true,
                // but only when the pk.mode is set to record_key.
                // This is because deleting a row from the table
                // requires the primary key be used as criteria.
                DELETE_ENABLED,
                ConfigDef.Type.BOOLEAN,
                DELETE_ENABLED_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                DELETE_ENABLED_DOC, WRITES_GROUP,
                3,
                ConfigDef.Width.SHORT,
                DELETE_ENABLED_DISPLAY);

        // Data Mapping
        CONFIG_DEF
            .define(
                TABLE_NAME_FORMAT,
                ConfigDef.Type.STRING,
                TABLE_NAME_FORMAT_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                TABLE_NAME_FORMAT_DOC,
                DATAMAPPING_GROUP,
                1,
                ConfigDef.Width.LONG,
                TABLE_NAME_FORMAT_DISPLAY
            )
            .define(
                TABLE_NAME_NORMALIZE,
                ConfigDef.Type.BOOLEAN,
                TABLE_NAME_NORMALIZE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                TABLE_NAME_NORMALIZE_DOC,
                DATAMAPPING_GROUP,
                2,
                ConfigDef.Width.LONG,
                TABLE_NAME_NORMALIZE_DISPLAY
            )
            .define(
                TOPICS_TO_TABLES_MAPPING,
                ConfigDef.Type.LIST,
                null,
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (Objects.isNull(value)
                                || ConfigDef.NO_DEFAULT_VALUE == value
                                || "".equals(value)) {
                            return;
                        }
                        assert value instanceof List;
                        try {
                            final Map<String, String> mapping = topicToTableMapping((List<String>) value);
                            if (Objects.isNull(mapping) || mapping.isEmpty()) {
                                throw new ConfigException(name, value, "Invalid topics to tables mapping");
                            }
                        } catch (final ArrayIndexOutOfBoundsException e) {
                            throw new ConfigException(name, value, "Invalid topics to tables mapping");
                        }
                    }
                },
                ConfigDef.Importance.MEDIUM,
                TOPICS_TO_TABLES_MAPPING_DOC,
                DATAMAPPING_GROUP,
                3,
                ConfigDef.Width.LONG,
                TOPICS_TO_TABLES_MAPPING_DISPLAY
            )
            .define(
                PK_MODE,
                ConfigDef.Type.STRING,
                PK_MODE_DEFAULT,
                EnumValidator.in(PrimaryKeyMode.values()),
                ConfigDef.Importance.HIGH,
                PK_MODE_DOC,
                DATAMAPPING_GROUP,
                4,
                ConfigDef.Width.MEDIUM,
                PK_MODE_DISPLAY
            )
            .define(
                PK_FIELDS,
                ConfigDef.Type.LIST,
                PK_FIELDS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                PK_FIELDS_DOC,
                DATAMAPPING_GROUP,
                5,
                ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
            )
            .define(
                FIELDS_WHITELIST,
                ConfigDef.Type.LIST,
                FIELDS_WHITELIST_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                FIELDS_WHITELIST_DOC,
                DATAMAPPING_GROUP,
                6,
                ConfigDef.Width.LONG,
                FIELDS_WHITELIST_DISPLAY
            );

        // DDL
        CONFIG_DEF
            .define(
                AUTO_CREATE,
                ConfigDef.Type.BOOLEAN,
                AUTO_CREATE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                AUTO_CREATE_DOC, DDL_GROUP,
                1,
                ConfigDef.Width.SHORT,
                AUTO_CREATE_DISPLAY
            )
            .define(
                AUTO_EVOLVE,
                ConfigDef.Type.BOOLEAN,
                AUTO_EVOLVE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                AUTO_EVOLVE_DOC, DDL_GROUP,
                2,
                ConfigDef.Width.SHORT,
                AUTO_EVOLVE_DISPLAY);

        // Retries
        CONFIG_DEF
            .define(
                MAX_RETRIES,
                ConfigDef.Type.INT,
                MAX_RETRIES_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                MAX_RETRIES_DOC,
                RETRIES_GROUP,
                1,
                ConfigDef.Width.SHORT,
                MAX_RETRIES_DISPLAY
            )
            .define(
                RETRY_BACKOFF_MS,
                ConfigDef.Type.INT,
                RETRY_BACKOFF_MS_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                RETRY_BACKOFF_MS_DOC,
                RETRIES_GROUP,
                2,
                ConfigDef.Width.SHORT,
                RETRY_BACKOFF_MS_DISPLAY);
    }

    public final String tableNameFormat;
    public final Map<String, String> topicsToTablesMapping;
    public final boolean tableNameNormalize;
    public final int batchSize;
    public final int maxRetries;
    public final int retryBackoffMs;
    public final boolean autoCreate;
    public final boolean autoEvolve;
    public final InsertMode insertMode;
    public final PrimaryKeyMode pkMode;
    public final List<String> pkFields;
    public final Set<String> fieldsWhitelist;
    public final TimeZone timeZone;
    public final boolean deleteEnabled;

    public JdbcSinkConfig(final Map<?, ?> props) {
        super(CONFIG_DEF, props);
        tableNameFormat = getString(TABLE_NAME_FORMAT).trim();
        tableNameNormalize = getBoolean(TABLE_NAME_NORMALIZE);
        topicsToTablesMapping = topicToTableMapping(getList(TOPICS_TO_TABLES_MAPPING));
        batchSize = getInt(BATCH_SIZE);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        autoCreate = getBoolean(AUTO_CREATE);
        autoEvolve = getBoolean(AUTO_EVOLVE);
        insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
        pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
        pkFields = getList(PK_FIELDS);
        fieldsWhitelist = new HashSet<>(getList(FIELDS_WHITELIST));
        final String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
        timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
        deleteEnabled = getBoolean(DELETE_ENABLED);
    }

    static Map<String, String> topicToTableMapping(final List<String> value) {
        return (Objects.nonNull(value))
                ? value.stream()
                    .map(s -> s.split(":"))
                    .collect(Collectors.toMap(e -> e[0], e -> e[1]))
                : Collections.emptyMap();
    }

    private static class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(final List<String> canonicalValues, final Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(final E[] enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length * 2);
            for (final E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toUpperCase());
                validValues.add(e.toString().toLowerCase());
            }
            return new EnumValidator(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(final String key, final Object value) {
            if (!validValues.contains(value)) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }

    public static void main(final String... args) {
        System.out.println("=========================================");
        System.out.println("JDBC Sink connector Configuration Options");
        System.out.println("=========================================");
        System.out.println();
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }

    public static void validateDeleteEnabled(final Config config) {
        // Collect all configuration values
        final Map<String, ConfigValue> configValues = config.configValues().stream()
                .collect(Collectors.toMap(ConfigValue::name, v -> v));

        // Check if DELETE_ENABLED is true
        final ConfigValue deleteEnabledConfigValue = configValues.get(JdbcSinkConfig.DELETE_ENABLED);
        final boolean deleteEnabled = (boolean) deleteEnabledConfigValue.value();

        // Check if PK_MODE is RECORD_KEY
        final ConfigValue pkModeConfigValue = configValues.get(JdbcSinkConfig.PK_MODE);
        final String pkMode = (String) pkModeConfigValue.value();

        if (deleteEnabled && !JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY.name().equalsIgnoreCase(pkMode)) {
            deleteEnabledConfigValue.addErrorMessage("Delete support only works with pk.mode=record_key");
        }
    }

    public static void validatePKModeAgainstPKFields(final Config config) {
        // Collect all configuration values
        final Map<String, ConfigValue> configValues = config.configValues().stream()
                .collect(Collectors.toMap(ConfigValue::name, v -> v));

        final ConfigValue pkModeConfigValue = configValues.get(JdbcSinkConfig.PK_MODE);
        final ConfigValue pkFieldsConfigValue = configValues.get(JdbcSinkConfig.PK_FIELDS);

        if (pkModeConfigValue == null || pkFieldsConfigValue == null) {
            return; // If either pkMode or pkFields are not configured, there's nothing to validate
        }

        final String pkMode = (String) pkModeConfigValue.value();
        final List<String> pkFields = (List<String>) pkFieldsConfigValue.value();

        if (pkMode == null) {
            return; // If pkMode is null, skip validation
        }

        switch (pkMode.toLowerCase()) {
            case "none":
                validateNoPKFields(pkFieldsConfigValue, pkFields);
                break;
            case "kafka":
                validateKafkaPKFields(pkFieldsConfigValue, pkFields);
                break;
            case "record_key":
            case "record_value":
                validatePKFieldsRequired(pkFieldsConfigValue, pkFields);
                break;
            default:
                pkFieldsConfigValue.addErrorMessage("Invalid pkMode value: " + pkMode);
                break;
        }
    }

    private static void validateNoPKFields(final ConfigValue pkFieldsConfigValue, final List<String> pkFields) {
        if (pkFields != null && !pkFields.isEmpty()) {
            pkFieldsConfigValue.addErrorMessage(
                    "Primary key fields should not be set when pkMode is 'none'."
            );
        }
    }

    private static void validateKafkaPKFields(final ConfigValue pkFieldsConfigValue, final List<String> pkFields) {
        if (pkFields == null || pkFields.size() != 3) {
            pkFieldsConfigValue.addErrorMessage(
                    "Primary key fields must be set with three fields "
                            + "(topic, partition, offset) when pkMode is 'kafka'."
            );
        }
    }

    private static void validatePKFieldsRequired(final ConfigValue pkFieldsConfigValue, final List<String> pkFields) {
        if (pkFields == null || pkFields.isEmpty()) {
            pkFieldsConfigValue.addErrorMessage(
                    "Primary key fields must be set when pkMode is 'record_key' or 'record_value'."
            );
        }
    }
}
