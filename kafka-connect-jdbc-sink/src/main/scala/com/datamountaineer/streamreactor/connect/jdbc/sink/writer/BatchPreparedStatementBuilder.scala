package com.datamountaineer.streamreactor.connect.jdbc.sink.writer

import java.sql.{Connection, PreparedStatement}

import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

trait PreparedStatementBuilder extends StrictLogging {
  def build(records: Seq[SinkRecord])(implicit connection: Connection): Seq[PreparedStatement]
}

/**
  * Creates a sql statement for all records sharing the same columns to be inserted.
  *
  * @param tableName       - The name of the database tabled
  * @param fieldsExtractor - An instance of the SinkRecord fields value extractor
  */
case class BatchedPreparedStatementBuilder(tableName: String,
                                           fieldsExtractor: StructFieldsDataExtractor) extends PreparedStatementBuilder {

  /**
    *
    * @param records    - The sequence of records to be inserted to the database
    * @param connection - The database connection instance
    * @return A sequence of PreparedStatement to be executed. It will batch the sql operation.
    */
  override def build(records: Seq[SinkRecord])(implicit connection: Connection): Seq[PreparedStatement] = {
    val preparedStatementsMap = records.foldLeft(Map.empty[String, PreparedStatement]) { (map, record) =>

      logger.debug(s"Received record from topic:${record.topic()} partition:${record.kafkaPartition()} and offset:${record.kafkaOffset()}")
      require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")

      val fieldsAndValues = fieldsExtractor.get(record.value.asInstanceOf[Struct])

      if (fieldsAndValues.nonEmpty) {
        val columns = fieldsAndValues.map(_._1)
        val statementKey = columns.mkString

        map.get(statementKey) match {
          case None =>
            val query = BuildInsertQueryFn(tableName, columns)
            val statement = connection.prepareStatement(query)
            PreparedStatementBindDataFn(statement, fieldsAndValues.map(_._2))
            statement.addBatch()
            map + (statementKey -> statement)

          case Some(statement) =>
            PreparedStatementBindDataFn(statement, fieldsAndValues.map(_._2))
            statement.addBatch()
            map
        }
      } else map
    }

    preparedStatementsMap.values.toSeq
  }
}


/**
  * Creates a sql statement for each record
  *
  * @param tableName       - The name of the database tabled
  * @param fieldsExtractor - An instance of the SinkRecord fields value extractor
  */
case class SinglePreparedStatementBuilder(tableName: String,
                                          fieldsExtractor: StructFieldsDataExtractor) extends PreparedStatementBuilder {

  /**
    * Creates a PreparedStatement for each SinkRecord
    *
    * @param records    - The sequence of records to be inserted to the database
    * @param connection - The database connection instance
    * @return A sequence of PreparedStatement to be executed. It will batch the sql operation.
    */
  override def build(records: Seq[SinkRecord])(implicit connection: Connection): Seq[PreparedStatement] = {
    records.flatMap { record =>

      logger.debug(s"Received record from topic:${record.topic()} partition:${record.kafkaPartition()} and offset:${record.kafkaOffset()}")
      require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")

      val fieldsAndValues = fieldsExtractor.get(record.value.asInstanceOf[Struct])

      if (fieldsAndValues.nonEmpty) {
        val columns = fieldsAndValues.map(_._1)
        val query = BuildInsertQueryFn(tableName, columns)
        val statement = connection.prepareStatement(query)
        PreparedStatementBindDataFn(statement, fieldsAndValues.map(_._2))
        Some(statement)
      }
      else None
    }
  }
}
