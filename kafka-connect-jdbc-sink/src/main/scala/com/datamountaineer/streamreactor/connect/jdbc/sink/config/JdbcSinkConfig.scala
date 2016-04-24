package com.datamountaineer.streamreactor.connect.jdbc.sink.config

import java.util

import io.confluent.common.config.ConfigDef.{Importance, Type}
import io.confluent.common.config.{AbstractConfig, ConfigDef}

object JdbcSinkConfig {

  val DATABASE_CONNECTION = "connect.jdbc.connection"
  val DATABASE_CONNECTION_DOC =
    """
      |Specifies the database connection
    """.stripMargin


  val DATABASE_TABLE = "connect.jdbc.table"
  val DATABASE_TABLE_DOC =
    """
      |Specifies the target database table to insert the data.
    """.stripMargin

  val DATABASE_IS_BATCHING = "connect.jdbc.sink.batching.enabled"
  val DATABASE_IS_BATCHING_DOC =
    """
      |Specifies if for a given sequence of SinkRecords are batched or not. <true> the data insert is batched;
      |<false> for each record a sql statement is created
    """.stripMargin

  val JAR_FILE = "connect.jdbc.sink.driver.jar"
  val JAR_FILE_DOC =
    """
      |Specifies the jar file to be loaded at runtime containing the jdbc driver
    """.stripMargin

  val DRIVER_MANAGER_CLASS = "connect.jdbc.sink.driver.manager.class"
  val DRIVER_MANAGER_CLASS_DOC =
    """
      |Specifies the canonical class name for the driver manager.
    """.stripMargin

  val FIELDS = "connect.jdbc.sink.fields"
  val FIELDS_DOC =
    """
      |Specifies which fields to consider when inserting the new Redis entry. If is not set it will use insert all the payload fields present in the payload.
      |Field mapping is supported; this way an avro record field can be inserted into a 'mapped' column.
      |Examples:
      |* fields to be used:field1,field2,field3
      |** fields with mapping: field1=alias1,field2,field3=alias3"
    """.stripMargin

  val ERROR_POLICY = "connect.jdbc.sink.error.policy"
  val ERROR_POLICY_DOC =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      | <noop> - the error is swallowed
      | <throw> - the error is allowed to propagate.
      | The error will be logged automatically
    """.stripMargin

  val config: ConfigDef = new ConfigDef()
    .define(DATABASE_CONNECTION, Type.STRING, Importance.HIGH, DATABASE_CONNECTION_DOC)
    .define(DATABASE_TABLE, Type.STRING, Importance.HIGH, DATABASE_CONNECTION_DOC)
    .define(JAR_FILE, Type.STRING, Importance.HIGH, JAR_FILE_DOC)
    .define(DRIVER_MANAGER_CLASS, Type.STRING, Importance.HIGH, DRIVER_MANAGER_CLASS_DOC)
    .define(FIELDS, Type.STRING, Importance.LOW, FIELDS_DOC)
    .define(DATABASE_IS_BATCHING, Type.BOOLEAN, true, Importance.LOW, DATABASE_IS_BATCHING_DOC)
    .define(ERROR_POLICY, Type.STRING, "throw", Importance.HIGH, ERROR_POLICY_DOC)
}

/**
  * <h1>RedisSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
class JdbcSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(JdbcSinkConfig.config, props) {
}