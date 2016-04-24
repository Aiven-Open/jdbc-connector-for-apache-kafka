package com.datamountaineer.streamreactor.connect.jdbc.sink

import java.sql.PreparedStatement


trait PreparedStatementBinder {
  def bind(index: Int, statement: PreparedStatement)
}

case class BooleanPreparedStatementBinder(value: Boolean) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setBoolean(index, value)
}

case class FloatPreparedStatementBinder(value: Float) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setFloat(index, value)
}

case class DoublePreparedStatementBinder(value: Double) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setDouble(index, value)
}

case class BytePreparedStatementBinder(value: Byte) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setByte(index, value)
}

case class ShortPreparedStatementBinder(value: Short) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setShort(index, value)
}

case class IntPreparedStatementBinder(value: Int) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setInt(index, value)
}

case class LongPreparedStatementBinder(value: Long) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setLong(index, value)
}

case class StringPreparedStatementBinder(value: String) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setString(index, value)
}

case class BytesPreparedStatementBinder(value: Array[Byte]) extends PreparedStatementBinder {
  override def bind(index: Int, statement: PreparedStatement): Unit = statement.setBytes(index, value)
}
