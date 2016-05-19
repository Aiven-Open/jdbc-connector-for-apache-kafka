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
  public final static String EXPORT_MAPPING_DOC = "Specifies to the mappings of topic to table. Additionally which fields" +
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
  public final static String DATABASE_CONNECTION_URI_DOC = "Specifies the JDBC database connection URI.";

  public final static String DATABASE_CONNECTION_USER = "connect.jdbc.connection.user";
  public final static String DATABASE_CONNECTION_USER_DOC = "Specifies the JDBC connection user.";

  public final static String DATABASE_CONNECTION_PASSWORD = "connect.jdbc.connection.password";
  public final static String DATABASE_CONNECTION_PASSWORD_DOC = "Specifies the JDBC connection password.";

  public final static String DATABASE_IS_BATCHING = "connect.jdbc.sink.batching.enabled";
  public final static String DATABASE_IS_BATCHING_DOC = "Specifies if a given sequence of SinkRecords are batched or not.\n" +
          "<true> the data insert is batched;\n" +
          "<false> for each record a sql statement is created.";

  public final static String ERROR_POLICY = "connect.jdbc.sink.error.policy";
  public final static String ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
          "There are two available options: \n" +
          "NOOP - the error is swallowed \n" +
          "THROW - the error is allowed to propagate. \n" +
          "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
          "The error will be logged automatically";

  public final static String MAX_RETRIES = "connect.jdbc.sink.max.retries";
  public final static String MAX_RETRIES_DOC = String.format("The maximum number of a message is retried. Only valid for %s" +
      " set to %s", ERROR_POLICY, ErrorPolicyEnum.RETRY.toString());
  private final static String MAX_RETRIES_DEFAULT = "10";

  public final static String RETRY_INTERVAL = "connect.jdbc.sink.retry.interval";
  public final static String RETRY_INTERVAL_DEFAULT = "60000";
  public final static String RETRY_INTERVAL_DOC = String.format("The time, in milliseconds between the Sink retry failed " +
      "inserts, if the %s is set to RETRY. Default is %s", ERROR_POLICY, RETRY_INTERVAL_DEFAULT);


  public final static String INSERT_MODE = "connect.jdbc.sink.mode";
  public final static String INSERT_MODE_DOC = "Specifies how the data should be landed into the RDBMS. Two options are \n" +
          "supported:INSERT(default value) and UPSERT.";

  private final static String DEFAULT_ERROR_POLICY = "throw";
  private final static String DEFAULT_INSERT_MODE = "INSERT";

  public final static ConfigDef getConfigDef() {
    return new ConfigDef()
            .define(DATABASE_CONNECTION_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_CONNECTION_URI_DOC)
            .define(DATABASE_CONNECTION_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_USER_DOC)
            .define(DATABASE_CONNECTION_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.LOW, DATABASE_CONNECTION_PASSWORD_DOC)
            .define(DATABASE_IS_BATCHING, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, DATABASE_IS_BATCHING_DOC)
            .define(ERROR_POLICY, ConfigDef.Type.STRING, DEFAULT_ERROR_POLICY, ConfigDef.Importance.HIGH, ERROR_POLICY_DOC)
            .define(INSERT_MODE, ConfigDef.Type.STRING, DEFAULT_INSERT_MODE, ConfigDef.Importance.HIGH, INSERT_MODE_DOC)
            .define(EXPORT_MAPPINGS, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, EXPORT_MAPPING_DOC)
            .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
            .define(RETRY_INTERVAL, ConfigDef.Type.INT, RETRY_INTERVAL_DEFAULT, ConfigDef.Importance.MEDIUM, RETRY_INTERVAL_DOC);
  }
}