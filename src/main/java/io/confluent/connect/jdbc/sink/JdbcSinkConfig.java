package io.confluent.connect.jdbc.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;

public class JdbcSinkConfig extends AbstractConfig {

  public enum InsertMode {
    INSERT,
    UPSERT
  }

  public enum PrimaryKeyMode {
    NONE,
    KAFKA,
    RECORD_KEY,
    RECORD_VALUE
  }

  private static final String TABLE_OVERRIDABLE_DOC = "Overridable at the table-level by using a '$table.' prefix.";
  private static final String TOPIC_OVERRIDABLE_DOC = "Overridable at the topic-level by using a '$topic.' prefix.";

  public static final String CONNECTION_URL = "connection.url";
  private static final String CONNECTION_URL_DOC = "JDBC connection URL. The protocol portion will be used for determining the SQL dialect to be used.";

  public static final String CONNECTION_USER = "connection.user";
  private static final String CONNECTION_USER_DOC = "JDBC connection user.";

  public static final String CONNECTION_PASSWORD = "connection.password";
  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";

  public static final String TABLE_NAME_FORMAT = "table.name.format";
  private static final String TABLE_NAME_FORMAT_DEFAULT = "%s";
  private static final String TABLE_NAME_FORMAT_DOC =
      "A Java format string, which may contain `%s` 0 or 1 times as a placeholder for the originating topic name. "
      + "For example, \"kafka_%s\" for the topic 'orders' will map to the table name 'kafka_orders'. " + TOPIC_OVERRIDABLE_DOC;

  public static final String MAX_RETRIES = "max.retries";
  private static final int MAX_RETRIES_DEFAULT = 10;
  private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
  private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";

  public static final String BATCH_SIZE = "batch.size";
  private static final int BATCH_SIZE_DEFAULT = 3000;
  private static final String BATCH_SIZE_DOC =
      "Specifies how many records to attempt to batch together for insertion, when possible. " + TABLE_OVERRIDABLE_DOC;

  public static final String AUTO_CREATE = "auto.create";
  private static final String AUTO_CREATE_DEFAULT = "false";
  private static final String AUTO_CREATE_DOC =
      "Whether to automatically create tables based on record schema if the sink table is found to be missing, by issuing a CREATE statement. "
      + TABLE_OVERRIDABLE_DOC;

  public static final String AUTO_EVOLVE = "auto.evolve";
  private static final String AUTO_EVOLVE_DEFAULT = "false";
  private static final String AUTO_EVOLVE_DOC =
      "Whether to automatically evolve table schema when record schema and table schema is found to be incompatible, by issuing an ALTER statement. "
      + TABLE_OVERRIDABLE_DOC;

  public static final String INSERT_MODE = "insert.mode";
  private static final String INSERT_MODE_DEFAULT = "insert";
  private static final String INSERT_MODE_DOC =
      "The insertion mode to use. Supported modes are 'insert' and 'upsert', with the latter translated to the appropriate upsert semantics for the target "
      + "database if it is supported. " + TABLE_OVERRIDABLE_DOC;

  public static final String PK_MODE = "pk.mode";
  private static final String PK_MODE_DEFAULT = "none";
  private static final String PK_MODE_DOC =
      "The primary key mode, also refer to 'pk.fields' documentation for interplay. Supported modes are: "
      + "'none' - no keys utilized; "
      + "'kafka' - Kafka coordinates are used as the PK; "
      + "'record_key' - field(s) from the record key are used, which may be a primitive or a struct; "
      + "'record_value' - field(s) from the record value are used, which must be a struct";

  public static final String PK_FIELDS = "pk.fields";
  private static final String PK_FIELDS_DEFAULT = "";
  private static final String PK_FIELDS_DOC =
      "List of comma-separated primary key field names. The interpretation of this config depends on the 'pk.mode': "
      + "'none' - ignored; "
      + "'kafka' - must be a trio representing the Kafka coordinates (topic: string, partition: int, offset: long); defaulting to (__connect_topic, __connect_partition, __connect_offset) if empty; "
      + "'record_key' - if empty, all fields from the key struct will be used, otherwise used to whitelist the desired fields - for primitive key only a single field name must be configured; "
      + "'record_value' - if empty, all fields from the value struct will be used, otherwise used to whitelist the desired fields.";

  private static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
    @Override
    public void ensureValid(String key, Object value) {
      switch (key) {
        case BATCH_SIZE:
        case MAX_RETRIES:
        case RETRY_BACKOFF_MS: {
          if ((int) value < 0) {
            throw new ConfigException(key, value, "Cannot be negative");
          }
          return;
        }
        case INSERT_MODE: {
          final String enumerator = ((String) value).toUpperCase();
          for (InsertMode insertMode : InsertMode.values()) {
            if (insertMode.name().equalsIgnoreCase(enumerator)) {
              return;
            }
          }
          throw new ConfigException(key, value, "Invalid insertion mode");
        }
        case PK_MODE: {
          final String enumerator = ((String) value).toUpperCase();
          for (PrimaryKeyMode pkMode : PrimaryKeyMode.values()) {
            if (pkMode.name().equalsIgnoreCase(enumerator)) {
              return;
            }
          }
          throw new ConfigException(key, value, "Invalid primary key mode");
        }
      }
    }

  };

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(CONNECTION_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, VALIDATOR, ConfigDef.Importance.HIGH, CONNECTION_URL_DOC)
      .define(CONNECTION_USER, ConfigDef.Type.STRING, null, VALIDATOR, ConfigDef.Importance.HIGH, CONNECTION_USER_DOC)
      .define(CONNECTION_PASSWORD, ConfigDef.Type.PASSWORD, null, VALIDATOR, ConfigDef.Importance.HIGH, CONNECTION_PASSWORD_DOC)
      .define(TABLE_NAME_FORMAT, ConfigDef.Type.STRING, TABLE_NAME_FORMAT_DEFAULT, VALIDATOR, ConfigDef.Importance.HIGH, TABLE_NAME_FORMAT_DOC)
      .define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, VALIDATOR, ConfigDef.Importance.HIGH, BATCH_SIZE_DOC)
      .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, VALIDATOR, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
      .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, VALIDATOR, ConfigDef.Importance.MEDIUM, RETRY_BACKOFF_MS_DOC)
      .define(AUTO_CREATE, ConfigDef.Type.BOOLEAN, AUTO_CREATE_DEFAULT, VALIDATOR, ConfigDef.Importance.MEDIUM, AUTO_CREATE_DOC)
      .define(AUTO_EVOLVE, ConfigDef.Type.BOOLEAN, AUTO_EVOLVE_DEFAULT, VALIDATOR, ConfigDef.Importance.MEDIUM, AUTO_EVOLVE_DOC)
      .define(INSERT_MODE, ConfigDef.Type.STRING, INSERT_MODE_DEFAULT, VALIDATOR, ConfigDef.Importance.MEDIUM, INSERT_MODE_DOC)
      .define(PK_MODE, ConfigDef.Type.STRING, PK_MODE_DEFAULT, VALIDATOR, ConfigDef.Importance.MEDIUM, PK_MODE_DOC)
      .define(PK_FIELDS, ConfigDef.Type.LIST, PK_FIELDS_DEFAULT, VALIDATOR, ConfigDef.Importance.MEDIUM, PK_FIELDS_DOC);

  public final String connectionUrl;
  public final String connectionUser;
  public final String connectionPassword;
  public final String tableNameFormat;
  public final int batchSize;
  public final int maxRetries;
  public final int retryBackoffMs;
  public final boolean autoCreate;
  public final boolean autoEvolve;
  public final InsertMode insertMode;
  public final PrimaryKeyMode pkMode;
  public final List<String> pkFields;

  public JdbcSinkConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    connectionUrl = getString(CONNECTION_URL);
    connectionUser = getString(CONNECTION_USER);
    connectionPassword = getString(CONNECTION_PASSWORD);
    tableNameFormat = getString(TABLE_NAME_FORMAT);
    batchSize = getInt(BATCH_SIZE);
    maxRetries = getInt(MAX_RETRIES);
    retryBackoffMs = getInt(RETRY_BACKOFF_MS);
    autoCreate = getBoolean(AUTO_CREATE);
    autoEvolve = getBoolean(AUTO_EVOLVE);
    insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
    pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
    pkFields = getList(PK_FIELDS);
  }

  public JdbcSinkConfig contextualConfig(String context) {
    final Map<String, Object> properties = originals();
    properties.putAll(originalsWithPrefix(context + "."));
    return new JdbcSinkConfig(properties);
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toRst());
  }

}