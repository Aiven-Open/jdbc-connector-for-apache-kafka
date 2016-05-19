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

import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Holds the Jdbc Sink settings
 */
public final class JdbcSinkSettings {
  private static final Logger logger = LoggerFactory.getLogger(JdbcSinkSettings.class);
  private final boolean batching;
  private final String connection;
  private final String user;
  private final String password;
  private final List<FieldsMappings> mappings;
  private final ErrorPolicyEnum errorPolicy;
  private final InsertModeEnum insertMode;
  private final int maxRetries;

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
                          InsertModeEnum insertMode,
                          int maxRetries) {
    ParameterValidator.notNullOrEmpty(connection, "connection");

    this.connection = connection;
    this.user = user;
    this.password = password;
    this.mappings = mappings;
    this.batching = batching;
    this.errorPolicy = errorPolicy;
    this.insertMode = insertMode;
    this.maxRetries = maxRetries;
  }

  public int getRetries() {
    return maxRetries;
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

    String rawExportMap = config.getString(JdbcSinkConfig.EXPORT_MAPPINGS);

    if (rawExportMap.isEmpty()) {
      throw new ConfigException(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set!");
    }

    final List<FieldsMappings> fieldsMappings = getTopicExportMappings(rawExportMap);
    //getTablesMappings(config);

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
          throw new ConfigException("Invalid configuration. UPSERT mode has been chosen but no primary keys have been " +
                  "provided for " + tm.getTableName());
      }
    }


    ErrorPolicyEnum policy = ErrorPolicyEnum.valueOf(config.getString(JdbcSinkConfig.ERROR_POLICY).toUpperCase());

    return new JdbcSinkSettings(
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION_URI),
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION_USER),
            config.getPassword(JdbcSinkConfig.DATABASE_CONNECTION_PASSWORD).value(),
            fieldsMappings,
            config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING),
            policy,
            insertMode,
            config.getInt(JdbcSinkConfig.MAX_RETRIES));
  }


  /**
   * Get a list of the export mappings for a mapping.
   *
   * @param raw the raw input
   * */
  public static List<FieldsMappings> getTopicExportMappings(String raw) {
    List<FieldsMappings> fieldsMappingsList = Lists.newArrayList();
    String[] rawMappings = raw.split("\\}");

    for (String rawMapping: rawMappings) {
      String[] split = rawMapping
                          .replace(",{", "")
                          .replace("{", "")
                          .replace("}", "")
                          .trim()
                          .split(";");

      if (split.length != 2) {
        throw new ConfigException(String.format(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set correctly, %s", rawMapping));
      }

      //get the topic to table mapping and field to column mapping
      String[] topicToTable = split[0].split(":");
      String[] fieldToCol = split[1].split(",");

      //enforce topic to table mapping
      if (topicToTable.length != 2) {
        throw new ConfigException(String.format(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set correctly, %s. Missing " +
            "either the table or topic.", rawMapping));
      }

      //break out table and topic
      String topic = topicToTable[0].trim();
      String table = topicToTable[1].trim();

      Map<String, FieldAlias> mappings = Maps.newHashMap();

      //all fields?
      if (!split[1].equals("*")) {
        //build field to column mappings
        for (String mapping : fieldToCol) {
          String[] colSplit = mapping.split("->");
          String fieldName = colSplit[0].trim();
          String colName;

          //no mapping but field added
          if (colSplit.length == 1) {
            colName = colSplit[0].trim();
          } else {
            colName = colSplit[1].trim();
          }
          mappings.put(fieldName, new FieldAlias(colName, true));
        }
      }

      fieldsMappingsList.add(new FieldsMappings(table, topic, true, mappings));
    }
    return fieldsMappingsList;
  }
}