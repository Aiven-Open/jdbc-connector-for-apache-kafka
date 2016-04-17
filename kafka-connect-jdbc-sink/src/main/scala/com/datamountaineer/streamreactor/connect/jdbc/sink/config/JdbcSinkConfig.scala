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

  val config: ConfigDef = new ConfigDef()
    .define(DATABASE_CONNECTION, Type.STRING, Importance.HIGH, DATABASE_CONNECTION_DOC)
    .define(JAR_FILE, Type.STRING, Importance.HIGH, JAR_FILE_DOC)
    .define(DRIVER_MANAGER_CLASS, Type.STRING, Importance.HIGH, DRIVER_MANAGER_CLASS_DOC)
    .define(FIELDS, Type.STRING, Importance.LOW, FIELDS_DOC)
}

/**
  * <h1>RedisSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
class JdbcSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(JdbcSinkConfig.config, props) {
}