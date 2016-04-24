package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer

import java.sql.Connection

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.{NoopErrorHandlingPolicy, ThrowErrorHandlingPolicy}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}


class NoopErrorHandlingPolicyTest extends WordSpec with Matchers with MockitoSugar {
  "NoopErrorHandlingPolicy" should {
    "swallow the exception" in {
      NoopErrorHandlingPolicy.handle(Seq.empty, new IllegalArgumentException)(mock[Connection])
    }
  }
}

class ThrowErrorHandlingPolicyTest extends WordSpec with Matchers with MockitoSugar {
  "ThrowErrorHandlingPolicy" should {
    "rethrow the exception" in {
      intercept[NumberFormatException] {
        ThrowErrorHandlingPolicy.handle(Seq.empty, new NumberFormatException)(mock[Connection])
      }
    }
  }
}