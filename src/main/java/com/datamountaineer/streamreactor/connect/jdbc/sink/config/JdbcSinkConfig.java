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

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * <h1>JdbcSinkConfig</h1>
 * <p>
 * Holds config, extends AbstractConfig.
 **/
public class JdbcSinkConfig extends AbstractConfig {

  public JdbcSinkConfig(Map<String, String> props) {
    super(config, props);
  }

  public final static String TABLE_MAPPINGS_FORMAT = "connect.jdbc.sink.table.%s.mappings";

  public final static String DATABASE_CONNECTION = "connect.jdbc.connection.uri";
  public final static String DATABASE_CONNECTION_DOC = "Specifies the JDBC database connection URI.";

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
  public final static String TOPIC_TABLE_MAPPING_DOC = "Specifies which topic maps to which table.Example:topic1=table1;topic2=table2 \n" +
          "For each table a field mappings need to be provided: connect.jdbc.sink.table.[table_name].mappings." +
          "If is not set it will use all the payload fields present in the payload as columns to be inserted.\n" +
          "Field mapping is supported; this allows a SinkRecord field to be mapped to a specific database column.\n" +
          "To specify a field is part of the primary key please enclose it between '[]':[FIELD1_PK],[FIELD2_PK]=ALIAS2_PK\n" +
          "Examples:\n" +
          "* fields to be used:field1,field2,field3 \n" +
          "** fields with mapping: field1=alias1,field2,field3=alias3";

  public final static ConfigDef config = new ConfigDef()
          .define(DATABASE_CONNECTION, Type.STRING, Importance.HIGH, DATABASE_CONNECTION_DOC)
          .define(DATABASE_CONNECTION_USER, Type.STRING, "", Importance.LOW, DATABASE_CONNECTION_USER_DOC)
          .define(DATABASE_CONNECTION_PASSWORD, Type.STRING, "", Importance.LOW, DATABASE_CONNECTION_PASSWORD_DOC)
          .define(JAR_FILE, Type.STRING, Importance.HIGH, JAR_FILE_DOC)
          .define(DRIVER_MANAGER_CLASS, Type.STRING, Importance.HIGH, DRIVER_MANAGER_CLASS_DOC)
          .define(TOPIC_TABLE_MAPPING, Type.STRING, Importance.LOW, TOPIC_TABLE_MAPPING_DOC)
          .define(DATABASE_IS_BATCHING, Type.BOOLEAN, true, Importance.LOW, DATABASE_IS_BATCHING_DOC)
          .define(ERROR_POLICY, Type.STRING, "throw", Importance.HIGH, ERROR_POLICY_DOC)
          .define(INSERT_MODE, Type.STRING, "INSERT", Importance.HIGH, INSERT_MODE_DOC);
}