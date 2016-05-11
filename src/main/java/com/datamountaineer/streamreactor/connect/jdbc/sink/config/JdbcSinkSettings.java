/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcDriverLoader;
import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.confluent.common.config.ConfigException;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Holds the Jdbc Sink settings
 */
public final class JdbcSinkSettings {
  private final boolean batching;
  private final String connection;
  private final String user;
  private final String password;
  private final List<FieldsMappings> mappings;
  private final ErrorPolicyEnum errorPolicy;
  private final InsertModeEnum insertMode;

  /**
   * Creates a new instance of JdbcSinkSettings
   *
   * @param connection  - The database connection string
   * @param mappings    - A list of payload field mappings
   * @param batching    - Specifies if the inserts should be batched
   * @param errorPolicy - Specifies how an error is handled
   * @param insertMode  - Specifies how the data is inserted into RDBMS
   */
  public JdbcSinkSettings(String connection,
                          String user,
                          String password,
                          List<FieldsMappings> mappings,
                          boolean batching,
                          ErrorPolicyEnum errorPolicy,
                          InsertModeEnum insertMode) {
    ParameterValidator.notNullOrEmpty(connection, "connection");

    this.connection = connection;
    this.user = user;
    this.password = password;
    this.mappings = mappings;
    this.batching = batching;
    this.errorPolicy = errorPolicy;
    this.insertMode = insertMode;
  }

  public String getConnection() {
    return connection;
  }

  public List<FieldsMappings> getMappings() {
    return mappings;
  }

  public boolean isBatching() {
    return batching;
  }

  public ErrorPolicyEnum getErrorPolicy() {
    return errorPolicy;
  }

  public InsertModeEnum getInsertMode() {
    return insertMode;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public String toString() {
    return String.format("JdbcSinkSettings(\n" +
                    "connection=%s\n" +
                    "table columns=%s\n" +
                    "error policy=%s\n" +
                    ")", connection, Joiner.on(";").join(mappings), errorPolicy.toString());
  }

  /**
   * Creates an instance of JdbcSinkSettings from a JdbcSinkConfig
   *
   * @param config : The map of all provided configurations
   * @return An instance of JdbcSinkSettings
   */
  public static JdbcSinkSettings from(final JdbcSinkConfig config) {

    final String driverClass = config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS);
    final File jarFile = new File(config.getString(JdbcSinkConfig.JAR_FILE));
    if (!jarFile.exists())
      throw new ConfigException(jarFile + " doesn't exist");

    JdbcDriverLoader.load(driverClass, jarFile);

    final List<FieldsMappings> fieldsMappings = getTablesMappings(config);

    InsertModeEnum insertMode;
    try {
      insertMode = InsertModeEnum.valueOf(config.getString(JdbcSinkConfig.INSERT_MODE));
    } catch (IllegalArgumentException e) {
      throw new ConfigException(JdbcSinkConfig.INSERT_MODE + " is not set correctly");
    }

    if (insertMode == InsertModeEnum.UPSERT) {
      for (FieldsMappings tm : fieldsMappings) {
        boolean hasPK = false;

        for (Map.Entry<String, FieldAlias> e : tm.getMappings().entrySet()) {
          if (e.getValue().isPrimaryKey()) {
            hasPK = true;
            break;
          }
        }
        if (!hasPK)
          throw new ConfigException("Invalid configuration. UPSERT mode has been chosen buy no primary keys have been " +
                  "provided for " + tm.getTableName());
      }
    }

    return new JdbcSinkSettings(
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION),
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION_USER),
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION_PASSWORD),
            fieldsMappings,
            config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING),
            ErrorPolicyEnum.valueOf(config.getString(JdbcSinkConfig.ERROR_POLICY)),
            insertMode);
  }

  private static List<FieldsMappings> getTablesMappings(final JdbcSinkConfig config) {
    final String fields = config.getString(JdbcSinkConfig.TOPIC_TABLE_MAPPING);
    if (fields == null || fields.trim().length() == 0)
      throw new ConfigException(JdbcSinkConfig.TOPIC_TABLE_MAPPING + " is not set correctly.");
    return Lists.transform(Lists.newArrayList(fields.split(",")), new Function<String, FieldsMappings>() {
      @Override
      public FieldsMappings apply(String input) {
        if (input.trim().length() == 0)
          throw new ConfigException("Empty topic to table mapping found");
        //input is topic=table
        final String[] arr = input.split("=");
        if (arr.length != 2)
          throw new ConfigException(input + " is not a valid topic to table mapping");

        final String tableName = arr[1].trim();
        if (tableName.length() == 0)
          throw new ConfigException(input + " is not a valid topic to table mapping");

        final String tableMappings = config.getString(String.format(JdbcSinkConfig.TABLE_MAPPINGS_FORMAT, tableName));
        if (tableMappings == null || tableMappings.trim().length() == 0) {
          return new FieldsMappings(tableName, arr[0].trim());
        }
        return FieldsMappings.from(arr[1].trim(), arr[0].trim(), tableMappings.trim());
      }
    });
  }
}