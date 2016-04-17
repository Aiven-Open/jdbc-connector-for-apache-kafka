package com.datamountaineer.streamreactor.connect.jdbc.sink.config

import java.io.File
import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}

class JdbcDriverLoaderTest extends WordSpec with Matchers {
  "JdbcDriverLoader" should {
    "throw an exception if the jar file doesn't exist" in {
      intercept[IllegalArgumentException] {
        JdbcDriverLoader("somedriver", new File("bogus.jar"))
      }
    }

    "load the oracle driver" in {
      val jar = Paths.get(getClass.getResource("/ojdbc7.jar").toURI).toAbsolutePath.toFile
      val driver = "oracle.jdbc.OracleDriver"
      JdbcDriverLoader(driver, jar) shouldBe true
      JdbcDriverLoader(driver, jar) shouldBe false
    }

    "load the microsoft driver" in {
      val jar = Paths.get(getClass.getResource("/sqljdbc4-4-4.0.jar").toURI).toAbsolutePath.toFile
      val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      JdbcDriverLoader(driver, jar) shouldBe true
      JdbcDriverLoader(driver, jar) shouldBe false
    }
  }
}
