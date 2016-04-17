package com.datamountaineer.streamreactor.connect

object Enumerators {
  def apply[T](enum: java.util.Enumeration[T]): Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = enum.hasMoreElements

    override def next(): T = enum.nextElement()
  }
}
