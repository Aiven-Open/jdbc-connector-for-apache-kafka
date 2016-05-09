/**
 * Copyright 2015 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcDriverLoader;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.DbDialectTypeEnum;
import com.google.common.base.Joiner;
import io.confluent.common.config.ConfigException;

import java.io.File;
import java.util.Map;


/**
 * Holds the Jdbc Sink settings
 */
public final class JdbcSinkSettings {
  private final String connection;
  private final String tableName;
  private final PayloadFields fields;
  private final boolean batching;
  private final DbDialectTypeEnum dialectType;
  private final ErrorPolicyEnum errorPolicy;

  public JdbcSinkSettings(String connection,
                          String tableName,
                          PayloadFields fields,
                          boolean batching,
                          ErrorPolicyEnum errorPolicy,
                          DbDialectTypeEnum dialectType) {
    this.connection = connection;
    this.tableName = tableName;
    this.fields = fields;
    this.batching = batching;
    this.errorPolicy = errorPolicy;
    this.dialectType = dialectType;
  }

  public String getConnection() {
    return connection;
  }

  public String getTableName() {
    return tableName;
  }

  public PayloadFields getFields() {
    return fields;
  }

  public boolean isBatching() {
    return batching;
  }

  public ErrorPolicyEnum getErrorPolicy() {
    return errorPolicy;
  }

  public DbDialectTypeEnum getDialectType() {
    return dialectType;
  }

  @Override
  public String toString() {
    return String.format("JdbcSinkSettings(\n" +
                    "connection=%s\n" +
                    "table name=%s\n" +
                    "fields=%s\n" +
                    "error policy=%s\n" +
                    "dialect type=%s\n" +
                    ")", connection, tableName, fields.toString(), errorPolicy.toString(), dialectType);
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
    if (!jarFile.exists()) {
      throw new ConfigException(jarFile + " doesn't exist");
    }

    JdbcDriverLoader.load(driverClass, jarFile);

    DbDialectTypeEnum dialectType;
    try {
      dialectType = DbDialectTypeEnum.valueOf(config.getString(JdbcSinkConfig.SQL_DIALECT));
    } catch (IllegalArgumentException e) {
      throw new ConfigException(JdbcSinkConfig.SQL_DIALECT + " has an invalid value. Valid options are:" +
          Joiner.on(",").join(DbDialectTypeEnum.values()));
    }

    final PayloadFields fields = PayloadFields.from(config.getString(JdbcSinkConfig.FIELDS));
    boolean hasPK = false;

    for (Map.Entry<String, FieldAlias> e : fields.getFieldsMappings().entrySet()) {
      if (e.getValue().isPrimaryKey()) {
        hasPK = true;
        break;
      }
    }

    //TODO: should pick up the dialect from the db connection !?
    if (hasPK && dialectType == DbDialectTypeEnum.NONE) {
      throw new ConfigException("Primary fields have been defined. You need to specify a SQL dialect to be used");
    }

    return new JdbcSinkSettings(
            config.getString(JdbcSinkConfig.DATABASE_CONNECTION),
            config.getString(JdbcSinkConfig.DATABASE_TABLE),
            fields,
            config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING),
            ErrorPolicyEnum.valueOf(config.getString(JdbcSinkConfig.ERROR_POLICY)),
            dialectType);
  }
}