package com.datamountaineer.streamreactor.connect.jdbc.sink.config

import java.io.File

import com.datamountaineer.streamreactor.connect.config.PayloadFields
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig._
import io.confluent.common.config.ConfigException

import scala.util.Try

/**
  * Holds the Jdbc Sink settings
  */
case class JdbcSinkSettings(connection: String,
                            tableName: String,
                            fields: PayloadFields,
                            batching: Boolean,
                            errorPolicy: ErrorPolicyEnum)


object JdbcSinkSettings {

  /**
    * Creates an instance of JdbcSinkSettings from a JdbcSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of JdbcSinkSettings
    */
  def apply(config: JdbcSinkConfig): JdbcSinkSettings = {

    val driverClass = config.getString(DRIVER_MANAGER_CLASS)
    val jarFile = new File(config.getString(JAR_FILE))
    if (!jarFile.exists())
      throw new ConfigException(s"$jarFile doesn't exist")

    JdbcDriverLoader(driverClass, jarFile)

    JdbcSinkSettings(
      config.getString(DATABASE_CONNECTION),
      config.getString(DATABASE_TABLE),
      PayloadFields(Try(config.getString(FIELDS)).toOption.flatMap(v => Option(v))),
      config.getBoolean(DATABASE_IS_BATCHING),
      ErrorPolicyEnum.valueOf(config.getString(ERROR_POLICY))
    )
  }

}
