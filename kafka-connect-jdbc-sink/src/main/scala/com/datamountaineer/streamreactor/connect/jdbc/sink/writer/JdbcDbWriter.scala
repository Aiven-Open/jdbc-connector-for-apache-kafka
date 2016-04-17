package com.datamountaineer.streamreactor.connect.jdbc.sink.writer

import com.datamountaineer.streamreactor.connect.StructFieldsExtractor
import com.datamountaineer.streamreactor.connect.sink._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._

/**
  * Responsible for taking a sequence of SinkRecord and write them to the database
  */
case class JdbcDbWriter(connection: String,
                        fieldsExtractor: StructFieldsExtractor) extends DbWriter with StrictLogging {

  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.warn("Received empty sequence of SinkRecord")
    else {
      records.foreach { record =>

        logger.debug(s"Received record from topic:${record.topic()} partition:${record.kafkaPartition()} and offset:${record.kafkaOffset()}")
        require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")

        val fieldsAndValues = fieldsExtractor.get(record.value.asInstanceOf[Struct])

        if (fieldsAndValues.nonEmpty) {
          val map = fieldsAndValues.toMap.asJava
          //TODO: here add the Statement and bindings
        }
      }
    }
  }

  override def close(): Unit = {

  }
}

