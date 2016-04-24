package com.datamountaineer.streamreactor.connect.jdbc.sink.writer

import java.sql.PreparedStatement

import com.datamountaineer.streamreactor.connect.jdbc.sink.PreparedStatementBinder

/**
  * Binds the SinkRecord entries to the sql PreparedStatement.
  */
object PreparedStatementBindDataFn {
  /**
    * Binds the values to the given PreparedStatement
    *
    * @param statement - the sql prepared statement to be executed
    * @param data      -  The SinkRecord values to be bound to the sql statement
    */
  def apply(statement: PreparedStatement, data: Seq[PreparedStatementBinder]): Unit = {
    data
      .zipWithIndex
      .foreach { case (valueBinder, index) => valueBinder.bind(index + 1, statement) }
  }
}
