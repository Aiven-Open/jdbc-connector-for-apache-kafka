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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * <h1>JdbcSinkConfig</h1>
 * <p>
 * Holds config, extends AbstractConfig.
 **/
public class JdbcSinkConfig extends AbstractConfig {


  public JdbcSinkConfig(Map<String, String> props) {
    super(getConfigDef(), props);
  }

  public JdbcSinkConfig(final ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }

  public final static String TABLE_MAPPINGS_FORMAT = "connect.jdbc.sink.table.%s.mappings";

  public final static String DATABASE_CONNECTION_URI = "connect.jdbc.connection.uri";
  public final static String DATABASE_CONNECTION_URI_DOC = "Specifies the JDBC database connection URI.";

  public final static String DATABASE = "connect.jdbc.sink.database";
  public final static String DATABASE_DOC = "The database to connect to. Used for table monitoring so must be set here " +
          String.format("and in %s", DATABASE_CONNECTION_URI);

  public final static String DATABASE_CONNECTION_USER = "connect.jdbc.connection.user";
  public final static String DATABASE_CONNECTION_USER_DOC = "Specifies the JDBC connection user.";

  public final static String DATABASE_CONNECTION_PASSWORD = "connect.jdbc.connection.password";
  public final static String DATABASE_CONNECTION_PASSWORD_DOC = "Specifies the JDBC connection password.";

  public final static String DATABASE_IS_BATCHING = "connect.jdbc.sink.batching.enabled";
  public final static String DATABASE_IS_BATCHING_DOC = "Specifies if a given sequence of SinkRecords are batched or not.\n" +
          "<true> the data insert is batched;\n" +
          "<false> for each record a sql statement is created.";

  public final static String JAR_FILE = "connect.jdbc.sink.driver.jar";
  public final static String JAR_FILE_DOC = "Specifies the jar file to be loaded at runtime containing the jdbc driver.";

  public final static String DRIVER_MANAGER_CLASS = "connect.jdbc.sink.driver.manager.class";
  public final static String DRIVER_MANAGER_CLASS_DOC = "Specifies the canonical class name for the driver manager.";

  public final static String ERROR_POLICY = "connect.jdbc.sink.error.policy";
  public final static String ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
          "There are two available options: <noop> - the error is swallowed <throw> - the error is allowed to propagate. \n" +
          "The error will be logged automatically";

  public final static String INSERT_MODE = "connect.jdbc.sink.mode";
  public final static String INSERT_MODE_DOC = "Specifies how the data should be landed into the RDBMS. Two options are \n" +
          "supported:INSERT(default value) and UPSERT.";

  public final static String TOPIC_TABLE_MAPPING = "connect.jdbc.sink.topics.to.tables";
  public final static String TOPIC_TABLE_MAPPING_DOC = "Specifies which topic maps to which table.Example:topic1=table1;topic2=table2 \n";

  private final static String DEFAULT_ERROR_POLICY = "throw";
  private final static String DEFAULT_INSERT_MODE = "INSERT";

  /*
  public final static ConfigDef config = new ConfigDef()
          .define(DATABASE_CONNECTION_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_CONNECTION_URI_DOC)
          .define(DATABASE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_DOC)
          .define(DATABASE_CONNECTION_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_USER_DOC)
          .define(DATABASE_CONNECTION_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_PASSWORD_DOC)
          .define(JAR_FILE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JAR_FILE_DOC)
          .define(DRIVER_MANAGER_CLASS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DRIVER_MANAGER_CLASS_DOC)
          .define(TOPIC_TABLE_MAPPING, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, TOPIC_TABLE_MAPPING_DOC)
          .define(DATABASE_IS_BATCHING, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, DATABASE_IS_BATCHING_DOC)
          .define(ERROR_POLICY, ConfigDef.Type.STRING, DEFAULT_ERROR_POLICY, ConfigDef.Importance.HIGH, ERROR_POLICY_DOC)
          .define(INSERT_MODE, ConfigDef.Type.STRING, DEFAULT_INSERT_MODE, ConfigDef.Importance.HIGH, INSERT_MODE_DOC);
*/
  public final static ConfigDef getConfigDef() {
    return new ConfigDef()
            .define(DATABASE_CONNECTION_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_CONNECTION_URI_DOC)
            .define(DATABASE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_DOC)
            .define(DATABASE_CONNECTION_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_USER_DOC)
            .define(DATABASE_CONNECTION_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_PASSWORD_DOC)
            .define(JAR_FILE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JAR_FILE_DOC)
            .define(DRIVER_MANAGER_CLASS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DRIVER_MANAGER_CLASS_DOC)
            .define(TOPIC_TABLE_MAPPING, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, TOPIC_TABLE_MAPPING_DOC)
            .define(DATABASE_IS_BATCHING, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, DATABASE_IS_BATCHING_DOC)
            .define(ERROR_POLICY, ConfigDef.Type.STRING, DEFAULT_ERROR_POLICY, ConfigDef.Importance.HIGH, ERROR_POLICY_DOC)
            .define(INSERT_MODE, ConfigDef.Type.STRING, DEFAULT_INSERT_MODE, ConfigDef.Importance.HIGH, INSERT_MODE_DOC);
  }
}