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

import com.datamountaineer.connector.config.Config;
import com.datamountaineer.connector.config.WriteModeEnum;
import com.datamountaineer.streamreactor.connect.jdbc.common.ParameterValidator;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds the Jdbc Sink settings
 */
public final class JdbcSinkSettings {
  private static final Logger logger = LoggerFactory.getLogger(JdbcSinkSettings.class);

  private final int batchSize;
  private final int maxRetries;
  private final int retryDelay;
  private final String connection;
  private final String user;
  private final String password;
  private final String schemaRegistryUrl;
  private final List<FieldsMappings> mappings;
  private final ErrorPolicyEnum errorPolicy;

  /**
   * Creates a new instance of JdbcSinkSettings
   *
   * @param connection        - The database connection string
   * @param mappings          - A list of payload field mappings
   * @param errorPolicy       - Specifies how an error is handled
   * @param maxRetries
   * @param schemaRegistryUrl
   * @param batchSize         - how big the batch size should be
   * @param retryDelay        -The time to wait before the operation is retried
   */
  public JdbcSinkSettings(String connection,
                          String user,
                          String password,
                          List<FieldsMappings> mappings,
                          ErrorPolicyEnum errorPolicy,
                          int maxRetries,
                          String schemaRegistryUrl,
                          int batchSize,
                          int retryDelay) {
    ParameterValidator.notNullOrEmpty(connection, "connection");
    if (retryDelay <= 0) {
      throw new IllegalArgumentException("Invalid retryDelay value. Needs to be greater than zero");
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Invalid batchSize value. Needs to be greater than zero");
    }

    this.connection = connection;
    this.user = user;
    this.password = password;
    this.mappings = mappings;
    this.errorPolicy = errorPolicy;
    this.maxRetries = maxRetries;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.batchSize = batchSize;
    this.retryDelay = retryDelay;
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

  public ErrorPolicyEnum getErrorPolicy() {
    return errorPolicy;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Set<String> getTableNames() {
    return Sets.newHashSet(Iterables.transform(mappings, new Function<FieldsMappings, String>() {
      @Override
      public String apply(FieldsMappings fieldsMappings) {
        return fieldsMappings.getTableName();
      }
    }));
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
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

    ErrorPolicyEnum policy = ErrorPolicyEnum.valueOf(config.getString(JdbcSinkConfig.ERROR_POLICY).toUpperCase());

    return new JdbcSinkSettings(
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION_URI),
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION_USER),
            config.getPassword(JdbcSinkConfig.DATABASE_CONNECTION_PASSWORD).value(),
            fieldsMappings,
            policy,
            config.getInt(JdbcSinkConfig.MAX_RETRIES),
            config.getString(JdbcSinkConfig.SCHEMA_REGISTRY_URL),
            config.getInt(JdbcSinkConfig.BATCH_SIZE),
            config.getInt(JdbcSinkConfig.RETRY_INTERVAL)
    );
  }

  /**
   * Get a list of the export mappings for a mapping.
   */
  private static List<FieldsMappings> getTopicExportMappings(JdbcSinkConfig config) {
    final String kcqlMappings = config.getString(JdbcSinkConfig.EXPORT_MAPPINGS);

    if (kcqlMappings == null || kcqlMappings.isEmpty()) {
      throw new ConfigException(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set!");
    }

    final String[] kcqlArray = kcqlMappings.split(";");

    final List<FieldsMappings> fieldsMappingsList = new ArrayList<>(kcqlArray.length);

    //go over our main mappings
    for (String kcql : kcqlArray) {
      try {
        final Config kcqlConfig = Config.parse(kcql);

        final Map<String, FieldAlias> fieldAliasMap = new HashMap<>();
        final HashSet<String> pkSet = new HashSet<>();
        final Iterator<String> iteratorPK = kcqlConfig.getPrimaryKeys();
        while (iteratorPK.hasNext()) {
          final String pk = iteratorPK.next();
          pkSet.add(pk);
          fieldAliasMap.put(pk, new FieldAlias(pk, true));
        }
        final Iterator<com.datamountaineer.connector.config.FieldAlias> iterator = kcqlConfig.getFieldAlias();

        while (iterator.hasNext()) {
          final com.datamountaineer.connector.config.FieldAlias fa = iterator.next();
          boolean isPrimaryKey = pkSet.contains(fa.getAlias());
          fieldAliasMap.put(fa.getField(), new FieldAlias(fa.getAlias(), isPrimaryKey));
        }

        //default primary keys
        if (kcqlConfig.isAutoCreate() && pkSet.size() == 0) {
          fieldAliasMap.put(FieldsMappings.CONNECT_TOPIC_COLUMN, new FieldAlias(FieldsMappings.CONNECT_TOPIC_COLUMN, true));
          fieldAliasMap.put(FieldsMappings.CONNECT_OFFSET_COLUMN, new FieldAlias(FieldsMappings.CONNECT_OFFSET_COLUMN, true));
          fieldAliasMap.put(FieldsMappings.CONNECT_PARTITION_COLUMN, new FieldAlias(FieldsMappings.CONNECT_PARTITION_COLUMN, true));
        }
        InsertModeEnum insertMode = InsertModeEnum.INSERT;
        if (kcqlConfig.getWriteMode() == WriteModeEnum.UPSERT) {
          insertMode = InsertModeEnum.UPSERT;
        }
        FieldsMappings fm = new FieldsMappings(kcqlConfig.getTarget(),
                kcqlConfig.getSource(),
                kcqlConfig.isIncludeAllFields(),
                insertMode,
                fieldAliasMap,
                kcqlConfig.isAutoCreate(),
                kcqlConfig.isAutoEvolve(),
                kcqlConfig.isEnableCapitalize());

        if (insertMode.equals(InsertModeEnum.UPSERT)
                && fm.autoCreateTable()
                && fm.getMappings().containsKey(FieldsMappings.CONNECT_PARTITION_COLUMN)) {
          throw new ConfigException("In order to use UPSERT mode and table AUTO-CREATE you need to define Primary Keys " +
                  "in your connect.jdbc.sink.export.mappings.");
        }

        logger.info("Creating field mapping:\n" + fm);
        fieldsMappingsList.add(fm);
      } catch (IllegalArgumentException ex) {
        throw new ConfigException(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set correctly." + System.lineSeparator()
                + ex.getMessage() + ":" + kcql, ex);
      }
    }
    return fieldsMappingsList;
  }

  public int getRetryDelay() {
    return 0;
  }
}