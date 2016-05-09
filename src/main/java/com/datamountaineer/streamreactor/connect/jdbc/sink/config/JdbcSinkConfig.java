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
    super(config, props);
  }

  public final static String DATABASE_CONNECTION = "connect.jdbc.connection";
  private final static String DATABASE_CONNECTION_DOC = "Specifies the database connection.";


  public final static String DATABASE_TABLE = "connect.jdbc.table";
  private final static String DATABASE_TABLE_DOC = "Specifies the target database table to insert the data.";


  public final static String DATABASE_IS_BATCHING = "connect.jdbc.sink.batching.enabled";
  private final static String DATABASE_IS_BATCHING_DOC = "Specifies if for a given sequence of SinkRecords are batched or not.\n" +
          "<true> the data insert is batched;\n" +
          "<false> for each record a sql statement is created.";

  public final static String JAR_FILE = "connect.jdbc.sink.driver.jar";
  private final static String JAR_FILE_DOC = "Specifies the jar file to be loaded at runtime containing the jdbc driver.";

  public final static String DRIVER_MANAGER_CLASS = "connect.jdbc.sink.driver.manager.class";
  private final static String DRIVER_MANAGER_CLASS_DOC = "Specifies the canonical class name for the driver manager.";

  public final static String FIELDS = "connect.jdbc.sink.fields";
  private final static String FIELDS_DOC = "Specifies which fields to consider when inserting the new JDBC entry.\n" +
          "If is not set it will use insert all the payload fields present in the payload.\n" +
          "Field mapping is supported; this way an avro record field can be inserted into a 'mapped' column.\n" +
          "To specify a field that is primary key you would have to specify [FIELD];this can still be aliased:\n" +
          "[FIELD_PK]=ALIAS_PK\n" +
          "Examples:\n" +
          "* fields to be used:field1,field2,field3 \n" +
          "** fields with mapping: field1=alias1,field2,field3=alias3";

  public final static String ERROR_POLICY = "connect.jdbc.sink.error.policy";
  private final static String ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
          "There are two available options: <noop> - the error is swallowed <throw> - the error is allowed to propagate. \n" +
          "The error will be logged automatically";

  public final static String SQL_DIALECT = "connect.jdbc.sink.sql.dialect";
  private final static String SQL_DIALECT_DOC = "Specifies which SQL dialect it should use. This is used for UPSERT.\n" +
          "Valid options are: mysql,oracle,sqlserver,sqlite";

  public final static ConfigDef config = new ConfigDef()
          .define(DATABASE_CONNECTION, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_CONNECTION_DOC)
          .define(DATABASE_TABLE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_TABLE_DOC)
          .define(JAR_FILE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JAR_FILE_DOC)
          .define(DRIVER_MANAGER_CLASS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DRIVER_MANAGER_CLASS_DOC)
          .define(FIELDS, ConfigDef.Type.STRING, "*", ConfigDef.Importance.LOW, FIELDS_DOC)
          .define(DATABASE_IS_BATCHING, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, DATABASE_IS_BATCHING_DOC)
          .define(ERROR_POLICY, ConfigDef.Type.STRING, "throw", ConfigDef.Importance.HIGH, ERROR_POLICY_DOC)
          .define(SQL_DIALECT, ConfigDef.Type.STRING, "NONE", ConfigDef.Importance.LOW, SQL_DIALECT_DOC);
}