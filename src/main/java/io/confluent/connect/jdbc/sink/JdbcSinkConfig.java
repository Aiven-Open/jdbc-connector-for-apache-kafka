/*
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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcSinkConfig extends AbstractConfig {

  public enum InsertMode {
    INSERT,
    UPSERT;
  }

  public enum PrimaryKeyMode {
    NONE,
    KAFKA,
    RECORD_KEY,
    RECORD_VALUE;
  }

  private static final String TABLE_OVERRIDABLE_DOC = "\nThis config is overridable at the table-level by using a '$table.' prefix.";
  private static final String TOPIC_OVERRIDABLE_DOC = "\nThis config is overridable at the topic-level by using a '$topic.' prefix.";

  public static final String CONNECTION_URL = "connection.url";
  private static final String CONNECTION_URL_DOC = "JDBC connection URL."
                                                   + "\nThe protocol portion will be used for determining the SQL dialect to be used.";

  public static final String CONNECTION_USER = "connection.user";
  private static final String CONNECTION_USER_DOC = "JDBC connection user.";

  public static final String CONNECTION_PASSWORD = "connection.password";
  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";

  public static final String TABLE_NAME_FORMAT = "table.name.format";
  private static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
  private static final String TABLE_NAME_FORMAT_DOC =
      "A format string for the destination table name, which may contain '${topic}' as a placeholder for the originating topic name."
      + "\nFor example, \"kafka_${topic}\" for the topic 'orders' will map to the table name 'kafka_orders'." + TOPIC_OVERRIDABLE_DOC;

  public static final String MAX_RETRIES = "max.retries";
  private static final int MAX_RETRIES_DEFAULT = 10;
  private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
  private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";

  public static final String BATCH_SIZE = "batch.size";
  private static final int BATCH_SIZE_DEFAULT = 3000;
  private static final String BATCH_SIZE_DOC =
      "Specifies how many records to attempt to batch together for insertion, when possible." + TABLE_OVERRIDABLE_DOC;

  public static final String AUTO_CREATE = "auto.create";
  private static final String AUTO_CREATE_DEFAULT = "false";
  private static final String AUTO_CREATE_DOC =
      "Whether to automatically create tables based on record schema if the sink table is found to be missing, by issuing a CREATE statement." + TABLE_OVERRIDABLE_DOC;

  public static final String AUTO_EVOLVE = "auto.evolve";
  private static final String AUTO_EVOLVE_DEFAULT = "false";
  private static final String AUTO_EVOLVE_DOC =
      "Whether to automatically evolve table schema when record schema and table schema is found to be incompatible, by issuing an ALTER statement." + TABLE_OVERRIDABLE_DOC;

  public static final String INSERT_MODE = "insert.mode";
  private static final String INSERT_MODE_DEFAULT = "insert";
  private static final String INSERT_MODE_DOC =
      "The insertion mode to use. Supported modes are 'insert' and 'upsert', with the latter translated to the appropriate upsert semantics for the target database if it is supported."
      + TABLE_OVERRIDABLE_DOC;

  public static final String PK_MODE = "pk.mode";
  private static final String PK_MODE_DEFAULT = "none";
  private static final String PK_MODE_DOC =
      "The primary key mode, also refer to 'pk.fields' documentation for interplay. Supported modes are: "
      + "\n'none' - no keys utilized."
      + "\n'kafka' - Kafka coordinates are used as the PK."
      + "\n'record_key' - field(s) from the record key are used, which may be a primitive or a struct."
      + "\n'record_value' - field(s) from the record value are used, which must be a struct.";

  public static final String PK_FIELDS = "pk.fields";
  private static final String PK_FIELDS_DEFAULT = "";
  private static final String PK_FIELDS_DOC =
      "List of comma-separated primary key field names. The interpretation of this config depends on the 'pk.mode':"
      + "\n'none' - ignored."
      + "\n'kafka' - must be a trio representing the Kafka coordinates (topic: string, partition: int, offset: long); defaulting to (__connect_topic, __connect_partition, __connect_offset) if empty."
      + "\n'record_key' - if empty, all fields from the key struct will be used, otherwise used to whitelist the desired fields - for primitive key only a single field name must be configured."
      + "\n'record_value' - if empty, all fields from the value struct will be used, otherwise used to whitelist the desired fields.";

  private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(CONNECTION_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, CONNECTION_URL_DOC)
      .define(CONNECTION_USER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CONNECTION_USER_DOC)
      .define(CONNECTION_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, CONNECTION_PASSWORD_DOC)
      .define(TABLE_NAME_FORMAT, ConfigDef.Type.STRING, TABLE_NAME_FORMAT_DEFAULT, ConfigDef.Importance.HIGH, TABLE_NAME_FORMAT_DOC)
      .define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, NON_NEGATIVE_INT_VALIDATOR, ConfigDef.Importance.HIGH, BATCH_SIZE_DOC)
      .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, NON_NEGATIVE_INT_VALIDATOR, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
      .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, NON_NEGATIVE_INT_VALIDATOR, ConfigDef.Importance.MEDIUM, RETRY_BACKOFF_MS_DOC)
      .define(AUTO_CREATE, ConfigDef.Type.BOOLEAN, AUTO_CREATE_DEFAULT, ConfigDef.Importance.MEDIUM, AUTO_CREATE_DOC)
      .define(AUTO_EVOLVE, ConfigDef.Type.BOOLEAN, AUTO_EVOLVE_DEFAULT, ConfigDef.Importance.MEDIUM, AUTO_EVOLVE_DOC)
      .define(INSERT_MODE, ConfigDef.Type.STRING, INSERT_MODE_DEFAULT, EnumValidator.in(InsertMode.values()), ConfigDef.Importance.MEDIUM, INSERT_MODE_DOC)
      .define(PK_MODE, ConfigDef.Type.STRING, PK_MODE_DEFAULT, EnumValidator.in(PrimaryKeyMode.values()), ConfigDef.Importance.MEDIUM, PK_MODE_DOC)
      .define(PK_FIELDS, ConfigDef.Type.LIST, PK_FIELDS_DEFAULT, ConfigDef.Importance.MEDIUM, PK_FIELDS_DOC);

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

  private static class EnumValidator implements ConfigDef.Validator {
    private final Set<String> validValues;

    private EnumValidator(Set<String> validValues) {
      this.validValues = validValues;
    }

    public static <E> EnumValidator in(E[] enumerators) {
      final HashSet<String> values = new HashSet<>();
      for (E e : enumerators) {
        values.add(e.toString().toUpperCase());
        values.add(e.toString().toLowerCase());
      }
      return new EnumValidator(values);
    }

    @Override
    public void ensureValid(String key, Object value) {
      if (!validValues.contains(value)) {
        throw new ConfigException(key, value, "Invalid enumerator");
      }
    }
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toRst());
  }

}