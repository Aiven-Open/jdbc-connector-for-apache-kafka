package com.datamountaineer.streamreactor.connect.sink


import org.apache.kafka.connect.sink.SinkRecord


/**
  * Defines the contrsct for inserting a new Hbase row for the connect sink record
  */
trait DbWriter extends AutoCloseable {
  def write(records: Seq[SinkRecord]): Unit
}
