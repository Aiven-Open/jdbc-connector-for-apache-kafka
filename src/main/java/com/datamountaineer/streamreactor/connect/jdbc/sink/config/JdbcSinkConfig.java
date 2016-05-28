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

  public final static String EXPORT_MAPPINGS = "connect.jdbc.sink.export.mappings";
  private final static String EXPORT_MAPPING_DOC = "Specifies to the mappings of topic to table. Additionally which fields" +
          "to select from the source topic and their mappings to columns in the target table." +
          "Multiple mappings can set comma separated wrapped in {}." +
          "" +
          "Examples:" +
          "{TOPIC1:TABLE1;field1->col1,field5->col5,field7->col10}" +
          "{TOPIC2:TABLE2;field1->,field2->}" +
          "{TOPIC3:TABLE3;*}" +
          "" +
          "The first mapping specifies map TOPIC1 to TABLE1 and select only field1, field2 and field7 from the topic payload. " +
          "Field1 is mapped to col1, field5 to col5 and field7 to col10." +
          "" +
          "The second mapping specifies TOPIC2 to TABLE2 and select only field1 and field2 from the topic payload" +
          "Map the fields to matching column names in TABLE2." +
          "" +
          "The third mapping specifies map TOPIC3 to TABLE3 and select all fields from the topic payload." +
          "" +
          "For fields mappings if `*` is supplied all fields are selected from the sink record. If not column name is provided" +
          "the fields name is used. Topic to table mapping must be explict, the table must be provided.";

  public final static String DATABASE_CONNECTION_URI = "connect.jdbc.connection.uri";
  private final static String DATABASE_CONNECTION_URI_DOC = "Specifies the JDBC database connection URI.";

  public final static String DATABASE_CONNECTION_USER = "connect.jdbc.connection.user";
  private final static String DATABASE_CONNECTION_USER_DOC = "Specifies the JDBC connection user.";

  public final static String DATABASE_CONNECTION_PASSWORD = "connect.jdbc.connection.password";
  private final static String DATABASE_CONNECTION_PASSWORD_DOC = "Specifies the JDBC connection password.";

  public final static String ERROR_POLICY = "connect.jdbc.sink.error.policy";
  private final static String ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
          "There are two available options: \n" +
          "NOOP - the error is swallowed \n" +
          "THROW - the error is allowed to propagate. \n" +
          "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
          "The error will be logged automatically";

  public final static String MAX_RETRIES = "connect.jdbc.sink.max.retries";
  private final static String MAX_RETRIES_DOC = String.format("The maximum number of a message is retried. Only valid for %s" +
          " set to %s", ERROR_POLICY, ErrorPolicyEnum.RETRY.toString());
  private final static String MAX_RETRIES_DEFAULT = "10";

  public final static String RETRY_INTERVAL = "connect.jdbc.sink.retry.interval";
  private final static int RETRY_INTERVAL_DEFAULT = 3000;
  private final static String RETRY_INTERVAL_DOC = String.format("The time, in milliseconds between the Sink retry failed " +
          "inserts, if the %s is set to RETRY. Default is %s", ERROR_POLICY, RETRY_INTERVAL_DEFAULT);


  public final static String INSERT_MODE = "connect.jdbc.sink.mode";
  private final static String INSERT_MODE_DOC = "Specifies how the data should be landed into the RDBMS. Two options are \n" +
          "supported:INSERT(default value) and UPSERT.";

  public final static String BATCH_SIZE = "connect.jdbc.sink.batch.size";
  private final static String BATCH_SIZE_DOC = "Specifies how many records to insert together at one time. If the connect framework " +
          "provides less records when it is calling the sink it won't wait to fulfill this value but rather execute it.";


  private final static int DEFAULT_BATCH_SIZE = 3000;
  private final static String DEFAULT_ERROR_POLICY = "THROW";
  private final static String DEFAULT_INSERT_MODE = "INSERT";

  public final static String TOPIC_TABLE_MAPPING = "connect.jdbc.sink.topics.to.tables";

  public final static String EVOLVE_TABLE_MAP = "connect.jdbc.sink.evolve.tables";
  private final static String EVOLVE_TABLE_MAP_DOC = "Comma separated list of tables on which to allow evolving schemas.";
  public final static String AUTO_CREATE_TABLE_MAP = "connect.jdbc.sink.auto.create.tables";
  private final static String AUTO_CREATE_TABLE_MAP_DOC = "List of tables to create automatically from topics.";

  public final static String SCHEMA_REGISTRY_URL = "connect.jdbc.sink.schema.registry.url";
  private final static String SCHEMA_REGISTRY_URL_DOC = "Url of the Schema registry";
  private final static String SCHEMA_REGISTRY_URL_DEFAULT = "http://localhost:8081";

  public final static String DEFAULT_PK_COL_NAME_VALUE = "__connect_auto_id";
  public final static String DEFAULT_PK_COL_NAME = "connect.jdbc.sink.pk.col.name";
  public final static String DEFAULT_PK_COL_DOC = "The name of the primary key column in the target table if " +
          AUTO_CREATE_TABLE_MAP + " is set and no fields are set the primary keys.";


  private static ConfigDef getConfigDef() {
    return new ConfigDef()
            .define(DATABASE_CONNECTION_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_CONNECTION_URI_DOC)
            .define(DATABASE_CONNECTION_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_USER_DOC)
            .define(DATABASE_CONNECTION_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_PASSWORD_DOC)
            .define(ERROR_POLICY, ConfigDef.Type.STRING, DEFAULT_ERROR_POLICY, ConfigDef.Importance.HIGH, ERROR_POLICY_DOC)
            .define(INSERT_MODE, ConfigDef.Type.STRING, DEFAULT_INSERT_MODE, ConfigDef.Importance.HIGH, INSERT_MODE_DOC)
            .define(BATCH_SIZE, ConfigDef.Type.INT, DEFAULT_BATCH_SIZE, ConfigDef.Importance.HIGH, BATCH_SIZE_DOC)
            .define(EXPORT_MAPPINGS, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, EXPORT_MAPPING_DOC)
            .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
            .define(RETRY_INTERVAL, ConfigDef.Type.INT, RETRY_INTERVAL_DEFAULT, ConfigDef.Importance.MEDIUM, RETRY_INTERVAL_DOC)
            .define(AUTO_CREATE_TABLE_MAP, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, AUTO_CREATE_TABLE_MAP_DOC)
            .define(EVOLVE_TABLE_MAP, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, EVOLVE_TABLE_MAP_DOC)
            .define(DEFAULT_PK_COL_NAME, ConfigDef.Type.STRING, DEFAULT_PK_COL_NAME_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_PK_COL_DOC)
            .define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, SCHEMA_REGISTRY_URL_DEFAULT, ConfigDef.Importance.HIGH, SCHEMA_REGISTRY_URL_DOC);
  }
}