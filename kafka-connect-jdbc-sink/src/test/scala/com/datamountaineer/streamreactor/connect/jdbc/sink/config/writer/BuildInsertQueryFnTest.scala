package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.BuildInsertQueryFn
import org.scalatest.{Matchers, WordSpec}

class BuildInsertQueryFnTest extends WordSpec with Matchers {

  "BuildInsertQueryFn" should {
    "throw an error if the map is empty" in {
      intercept[IllegalArgumentException] {
        BuildInsertQueryFn("sometable", Seq.empty)
      }
    }
    "throw anr error if the tableName is empty" in {
      intercept[IllegalArgumentException] {
        BuildInsertQueryFn("  ", Seq("a"))
      }
    }
    "return the correct statement" in {
      val query = BuildInsertQueryFn("customers", Seq("age", "firstName", "lastName"))

      query shouldBe "INSERT INTO customers(age,firstName,lastName) VALUES(?,?,?)"
    }
  }
}


