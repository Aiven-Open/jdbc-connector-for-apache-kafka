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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.confluent.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public Set<String> getTableNames() {
    return Sets.newHashSet(Iterables.transform(mappings, new Function<FieldsMappings, String>() {
      @Override
      public String apply(FieldsMappings fieldsMappings) {
        return fieldsMappings.getTableName();
      }
    }));
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

    final List<FieldsMappings> fieldsMappings = getTopicExportMappings(config);

    InsertModeEnum insertMode;
    try {
      insertMode = InsertModeEnum.valueOf(config.getString(JdbcSinkConfig.INSERT_MODE).toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new ConfigException(JdbcSinkConfig.INSERT_MODE + " is not set correctly");
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
   */
  private static List<FieldsMappings> getTopicExportMappings(JdbcSinkConfig config) {
    String rawExportMap = config.getString(JdbcSinkConfig.EXPORT_MAPPINGS).replaceAll("\\s+", "");

    if (rawExportMap.isEmpty()) {
      throw new ConfigException(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set!");
    }

    //get the auto create map
    final String autoCreateRaw = config.getString(JdbcSinkConfig.AUTO_CREATE_TABLE_MAP).replaceAll("\\s+", "");
    final String evolveRaw = config.getString(JdbcSinkConfig.EVOLVE_TABLE_MAP).replaceAll("\\s+", "");

    List<FieldsMappings> fieldsMappingsList = Lists.newArrayList();
    String[] rawMappings = rawExportMap.split("\\}");

    //go over our main mappings
    for (String rawMapping : rawMappings) {
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

      //auto create the table from the topic
      final boolean autoCreateTable = autoCreateRaw.contains(topic);
      //allow evolving topics
      final boolean evolveTableSchema = evolveRaw.contains(topic);
      logger.info(String.format("Setting auto create table to %s and schema evolution to %s for topic %s and table %s",
              autoCreateTable, evolveTableSchema, topic, table));

      //auto create is true so try and get pk fields
      Set<String> pkCols = Sets.newHashSet();
      if (autoCreateTable) {
        int mapStart = autoCreateRaw.indexOf("{" + topic);
        //if no fields were specified introduce the defaul PK colum
        if (mapStart > 0) {
          int delimiterIndex = autoCreateRaw.indexOf(":", mapStart);
          if (delimiterIndex < 0) {
            throw new ConfigException("Invalid configuration for " + JdbcSinkConfig.AUTO_CREATE_TABLE_MAP + ". Make sure you " +
                    "provide the value in the format {..:..}");
          }
          int mapEnd = autoCreateRaw.indexOf("}", mapStart);
          if (mapEnd < 0) {
            throw new ConfigException("Invalid configuration for " + JdbcSinkConfig.AUTO_CREATE_TABLE_MAP + ". Make sure you " +
                    "provide the value in the format {..:..}");
          }
          String[] pks = autoCreateRaw.trim().substring(delimiterIndex + 1, mapEnd).split(",");
          for (String pk : pks) {
            if (!pk.isEmpty()) pkCols.add(pk);
          }
        } else {
          pkCols.add(FieldsMappings.CONNECT_AUTO_ID_COLUMN);
        }
      }

      //now get fields mappings
      Map<String, FieldAlias> mappings = Maps.newHashMap();
      Boolean allFields = split[1].equals("*");

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

          boolean pk = pkCols.contains(colName);
          mappings.put(fieldName, new FieldAlias(colName, pk));
        }
      } else {
        //all fields mode but need to know the pks if any set.
        for (String pk : pkCols) {
          mappings.put(pk, new FieldAlias(pk, true));
        }
      }

      //check pks are in our fields selection if not allFields
      if (!allFields) {
        for (String pk : pkCols) {
          if (pk != FieldsMappings.CONNECT_AUTO_ID_COLUMN && !mappings.containsKey(pk)) {
            throw new ConfigException(String.format("Primary key %s mapping specified that does not exist in field selection %s.",
                    pk, rawExportMap));
          }
        }
      }

      FieldsMappings fm = new FieldsMappings(table, topic, allFields, mappings, autoCreateTable, evolveTableSchema,
              PrimaryKeyMode.FIELD);
      fieldsMappingsList.add(fm);
    }
    return fieldsMappingsList;
  }
}