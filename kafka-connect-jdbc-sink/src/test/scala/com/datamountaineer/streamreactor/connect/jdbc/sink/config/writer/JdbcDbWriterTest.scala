package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer

import java.nio.file.Paths

import com.datamountaineer.streamreactor.connect.config.PayloadFields
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.{ErrorPolicyEnum, JdbcDriverLoader, JdbcSinkSettings}
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer._
import org.scalatest.{Matchers, WordSpec}


class JdbcDbWriterTest extends WordSpec with Matchers {
  JdbcDriverLoader("org.sqlite.JDBC", Paths.get(getClass.getResource("/sqlite-jdbc-3.8.11.2.jar").toURI).toFile)

  "JdbcDbWriter" should {
    "writer should use batching" in {
      val settings = JdbcSinkSettings("jdbc:sqlite:sample.db", "tableA", PayloadFields(true, Map.empty), true, ErrorPolicyEnum.NOOP)
      val writer = JdbcDbWriter(settings)

      writer.connectionStr shouldBe settings.connection
      writer.statementBuilder.getClass shouldBe classOf[BatchedPreparedStatementBuilder]
    }

    "the writer works without batch insert " in {
      val settings = JdbcSinkSettings("jdbc:sqlite:sample.db", "tableA", PayloadFields(true, Map.empty), false, ErrorPolicyEnum.NOOP)
      val writer = JdbcDbWriter(settings)

      writer.connectionStr shouldBe settings.connection
      writer.statementBuilder.getClass shouldBe classOf[SinglePreparedStatementBuilder]
    }

    "the writer error policy should be Noop" in {
      val settings = JdbcSinkSettings("jdbc:sqlite:sample.db", "tableA", PayloadFields(true, Map.empty), true, ErrorPolicyEnum.NOOP)
      val writer = JdbcDbWriter(settings)

      writer.connectionStr shouldBe settings.connection
      writer.errorHandlingPolicy.getClass shouldBe NoopErrorHandlingPolicy.getClass
    }

    "the writer error policy should throw the exception" in {
      val settings = JdbcSinkSettings("jdbc:sqlite:sample.db", "tableA", PayloadFields(true, Map.empty), true, ErrorPolicyEnum.THROW)
      val writer = JdbcDbWriter(settings)

      writer.connectionStr shouldBe settings.connection
      writer.errorHandlingPolicy.getClass shouldBe ThrowErrorHandlingPolicy.getClass
    }
  }
}
