package com.datamountaineer.streamreactor.connect.jdbc.sink.writer

/**
  * Prepares an instance of PrepareStatement for inserting into the given table the fields present in the map
  */
object BuildInsertQueryFn {
  def apply(tableName: String, columns: Seq[String]): String = {
    require(tableName != null && tableName.trim.length > 0, s"'$tableName' is not a valid")
    require(columns.nonEmpty, "Invalid list of columns")

    s"INSERT INTO $tableName(${columns.mkString(",")}) VALUES(${Seq.fill(columns.size)("?").mkString(",")})"
  }
}
