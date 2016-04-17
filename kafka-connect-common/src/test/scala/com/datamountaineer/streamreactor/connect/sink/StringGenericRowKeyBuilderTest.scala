package com.datamountaineer.streamreactor.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}


class StringGenericRowKeyBuilderTest extends WordSpec with Matchers {
  "StringGenericRowKeyBuilder" should {
    "use the topic, partition and offset to make the key" in {

      val topic = "sometopic"
      val partition = 2
      val offset = 1243L
      val sinkRecord = new SinkRecord(topic, partition, Schema.INT32_SCHEMA, 345, Schema.STRING_SCHEMA, "", offset)

      val keyBuilder = new StringGenericRowKeyBuilder()
      val expected = Seq(topic, partition, offset).mkString(".")
      keyBuilder.build(sinkRecord) shouldBe expected
    }
  }
}
