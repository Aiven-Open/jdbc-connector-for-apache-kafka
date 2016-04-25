package com.datamountaineer.streamreactor.connect.jdbc.sink.writer

import java.sql.SQLException

import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.{ErrorPolicyEnum, JdbcSinkSettings}
import com.datamountaineer.streamreactor.connect.sink._
import com.typesafe.scalalogging.slf4j.StrictLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

/**
  *
  * Responsible for taking a sequence of SinkRecord and write them to the database
  *
  * @param connectionStr       - The database connection string
  * @param statementBuilder    - Returns a sequence of PreparedStatement to process
  * @param errorHandlingPolicy - An instance of the error handling approach
  */
case class JdbcDbWriter(connectionStr: String,
                        statementBuilder: PreparedStatementBuilder,
                        errorHandlingPolicy: ErrorHandlingPolicy) extends DbWriter with StrictLogging {

  private val config = new HikariConfig()
  config.setJdbcUrl(connectionStr)
  //config.setUsername("bart");
  //config.setPassword("51mp50n");
  private val ds = new HikariDataSource(config)
  //val connection = DriverManager.getConnection(connectionStr)

  /**
    * Writes the given records to the database
    *
    * @param records - The sequence of records to insert
    */
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.warn("Received empty sequence of SinkRecord")
    else {

      val connection = ds.getConnection
      try {
        val statemens = statementBuilder.build(records)(connection)
        if (statemens.nonEmpty) {
          try {
            //begin transaction
            connection.setAutoCommit(false)
            statemens.foreach { statement =>
              statement.execute()
            }
            //commit the transaction
            connection.commit()
          }
          catch {
            case sqlException: SQLException =>
              logger.error(
                s"""
                   | Following error has occurred inserting data starting at
                   | topic:${records.head.topic()}
                   | offset:${records.head.kafkaOffset()}
                   | partition:${records.head.kafkaPartition()}""".stripMargin)

              //rollback the transaction
              connection.rollback()
              errorHandlingPolicy.handle(records, sqlException)(connection)
          }
          finally {
            Try {
              statemens.foreach(_.close())
            }
          }
        }
      }
      finally {
        if (connection == null) {
          connection.close()
        }
      }
    }
  }


  override def close(): Unit = {
    ds.close()
  }
}

object JdbcDbWriter {
  def apply(settings: JdbcSinkSettings): JdbcDbWriter = {
    val fieldsValuesExtractor = StructFieldsDataExtractor(settings.fields.includeAllFields, settings.fields.fieldsMappings)
    val statementBuilder = if (settings.batching) {
      BatchedPreparedStatementBuilder(settings.tableName, fieldsValuesExtractor)
    } else {
      SinglePreparedStatementBuilder(settings.tableName, fieldsValuesExtractor)
    }

    val errorHandlingPolicy = settings.errorPolicy match {
      case ErrorPolicyEnum.NOOP => NoopErrorHandlingPolicy
      case ErrorPolicyEnum.THROW => ThrowErrorHandlingPolicy
    }

    JdbcDbWriter(
      settings.connection,
      statementBuilder,
      errorHandlingPolicy
    )
  }
}