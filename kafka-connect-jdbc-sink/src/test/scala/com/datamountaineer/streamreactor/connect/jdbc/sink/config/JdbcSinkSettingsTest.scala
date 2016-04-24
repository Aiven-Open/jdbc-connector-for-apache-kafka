package com.datamountaineer.streamreactor.connect.jdbc.sink.config

import java.nio.file.Paths

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class JdbcSinkSettingsTest extends WordSpec with Matchers with MockitoSugar {
  "JdbcSinkSettings" should {
    "return an instance of JdbcSinkSettings from JdbcSinkConfig" in {
      val config = mock[JdbcSinkConfig]
      val connection = "somedbconnection"
      val table = "the_table"
      when(config.getString(JdbcSinkConfig.JAR_FILE)).thenReturn(Paths.get(getClass.getResource("/sqlite-jdbc-3.8.11.2.jar").toURI).toAbsolutePath.toString)
      when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC")
      when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection)
      when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table)
      when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true)
      when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP")

      val settings = JdbcSinkSettings(config)

      settings.connection shouldBe connection
      settings.tableName shouldBe table
      settings.fields.includeAllFields shouldBe true
      settings.fields.fieldsMappings shouldBe Map.empty
    }

    "return an instance of JdbcSinkSettings from JdbcSinkConfig with only some of the payload fields taken into account" in {
      val config = mock[JdbcSinkConfig]
      val connection = "somedbconnection"
      val table ="one_table"
      when(config.getString(JdbcSinkConfig.JAR_FILE)).thenReturn(Paths.get(getClass.getResource("/sqlite-jdbc-3.8.11.2.jar").toURI).toAbsolutePath.toString)
      when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC")
      when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection)
      when(config.getString(JdbcSinkConfig.FIELDS)).thenReturn("field1,field2=alias2,field3")
      when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table)
      when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true)
      when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP")

      val settings = JdbcSinkSettings(config)

      settings.connection shouldBe connection
      settings.tableName shouldBe table
      settings.fields.includeAllFields shouldBe false
      settings.fields.fieldsMappings shouldBe Map("field1" -> "field1", "field2" -> "alias2", "field3" -> "field3")
    }

    "return an instance of JdbcSinkSettings from JdbcSinkConfig with all fields included and the given mappings" in {
      val config = mock[JdbcSinkConfig]
      val connection = "somedbconnection"
      val table="targettable"
      when(config.getString(JdbcSinkConfig.JAR_FILE)).thenReturn(Paths.get(getClass.getResource("/sqlite-jdbc-3.8.11.2.jar").toURI).toAbsolutePath.toString)
      when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC")
      when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection)
      when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table)
      when(config.getString(JdbcSinkConfig.FIELDS)).thenReturn("*,field2=alias2")
      when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true)
      when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP")

      val settings = JdbcSinkSettings(config)

      settings.connection shouldBe connection
      settings.fields.includeAllFields shouldBe true
      settings.tableName shouldBe table
      settings.fields.fieldsMappings shouldBe Map("field2" -> "alias2")
    }
  }
}
