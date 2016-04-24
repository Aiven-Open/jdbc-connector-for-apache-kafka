package com.datamountaineer.streamreactor.connect.jdbc.sink.writer

import java.sql.Connection

import org.apache.kafka.connect.sink.SinkRecord


/**
  * Defines the approach to take when the db operation fails inserting the new records
  */
trait ErrorHandlingPolicy {
  /**
    * Called when the sql operation to insert the SinkRecords fails
    *
    * @param records    The list of records received by the SinkTask
    * @param error      - The error raised when the insert failed
    * @param connection - The database connection instance
    */
  def handle(records: Seq[SinkRecord], error: Throwable)(implicit connection: Connection)
}


/**
  * The policy propagates the error further
  */
case object ThrowErrorHandlingPolicy extends ErrorHandlingPolicy {
  override def handle(records: Seq[SinkRecord], error: Throwable)(implicit connection: Connection): Unit = {
    throw error
  }
}

/**
  * The policy swallows the exception
  */
case object NoopErrorHandlingPolicy extends ErrorHandlingPolicy {
  override def handle(records: Seq[SinkRecord], error: Throwable)(implicit connection: Connection): Unit = {}
}
