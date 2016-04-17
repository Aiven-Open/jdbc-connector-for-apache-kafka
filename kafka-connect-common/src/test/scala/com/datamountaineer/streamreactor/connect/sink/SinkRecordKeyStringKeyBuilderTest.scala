package com.datamountaineer.streamreactor.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}


class SinkRecordKeyStringKeyBuilderTest extends WordSpec with Matchers {
  val keyRowKeyBuilder = new SinkRecordKeyStringKeyBuilder()

  "SinkRecordKeyStringKeyBuilder" should {

    "create the right key from the Schema key value - Byte" in {
      val b = 123.toByte
      val sinkRecord = new SinkRecord("", 1, Schema.INT8_SCHEMA, b, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord) shouldBe "123"

    }
    "create the right key from the Schema key value - String" in {
      val s = "somekey"
      val sinkRecord = new SinkRecord("", 1, Schema.STRING_SCHEMA, s, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord) shouldBe s
    }

    "create the right key from the Schema key value - Bytes" in {
      val bArray = Array(23.toByte, 24.toByte, 242.toByte)
      val sinkRecord = new SinkRecord("", 1, Schema.BYTES_SCHEMA, bArray, Schema.FLOAT64_SCHEMA, Nil, 0)
      keyRowKeyBuilder.build(sinkRecord) shouldBe bArray.toString
    }
    "create the right key from the Schema key value - Boolean" in {
      val bool = true
      val sinkRecord = new SinkRecord("", 1, Schema.BOOLEAN_SCHEMA, bool, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord) shouldBe "true"

    }
  }
}
