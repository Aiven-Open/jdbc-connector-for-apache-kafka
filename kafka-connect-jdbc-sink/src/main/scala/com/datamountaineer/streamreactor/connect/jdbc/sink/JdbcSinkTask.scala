package com.datamountaineer.streamreactor.connect.jdbc.sink

import java.util

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.{JdbcSinkConfig, JdbcSinkSettings}
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

/**
  * <h1>JdbcSinkTask</h1>
  *
  * Kafka Connect Jdbc sink task. Called by framework to put records to the
  * target sink
  **/
class JdbcSinkTask extends SinkTask with StrictLogging {
  var writer: Option[JdbcDbWriter] = None

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(

      """
        | ____              __                                                 __
        |/\  _`\           /\ \__              /'\_/`\                        /\ \__           __
        |\ \ \/\ \     __  \ \ ,_\    __      /\      \    ___   __  __    ___\ \ ,_\    __   /\_\    ___      __     __   _ __
        | \ \ \ \ \  /'__`\ \ \ \/  /'__`\    \ \ \__\ \  / __`\/\ \/\ \ /' _ `\ \ \/  /'__`\ \/\ \ /' _ `\  /'__`\ /'__`\/\`'__\
        |  \ \ \_\ \/\ \L\.\_\ \ \_/\ \L\.\_   \ \ \_/\ \/\ \L\ \ \ \_\ \/\ \/\ \ \ \_/\ \L\.\_\ \ \/\ \/\ \/\  __//\  __/\ \ \/
        |   \ \____/\ \__/.\_\\ \__\ \__/.\_\   \ \_\\ \_\ \____/\ \____/\ \_\ \_\ \__\ \__/.\_\\ \_\ \_\ \_\ \____\ \____\\ \_\
        |    \/___/  \/__/\/_/ \/__/\/__/\/_/    \/_/ \/_/\/___/  \/___/  \/_/\/_/\/__/\/__/\/_/ \/_/\/_/\/_/\/____/\/____/ \/_/
        |                                         by Stefan Bocutiu
        | _____      __  __                  ____                __
        |/\___ \    /\ \/\ \                /\  _`\   __        /\ \
        |\/__/\ \   \_\ \ \ \____    ___    \ \,\L\_\/\_\    ___\ \ \/'\
        |   _\ \ \  /'_` \ \ '__`\  /'___\   \/_\__ \\/\ \ /' _ `\ \ , <
        |  /\ \_\ \/\ \L\ \ \ \L\ \/\ \__/     /\ \L\ \ \ \/\ \/\ \ \ \\`\
        |  \ \____/\ \___,_\ \_,__/\ \____\    \ `\____\ \_\ \_\ \_\ \_\ \_\
        |   \/___/  \/__,_ /\/___/  \/____/     \/_____/\/_/\/_/\/_/\/_/\/_/
        |

      """.stripMargin)

    JdbcSinkConfig.config.parse(props)
    val sinkConfig = new JdbcSinkConfig(props)
    val settings = JdbcSinkSettings(sinkConfig)
    logger.info(
      s"""Settings:
          |$settings
      """.stripMargin)


    writer = Some(JdbcDbWriter(settings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.info("Empty list of records received.")
    else {
      require(writer.nonEmpty, "Writer is not set!")
      writer.foreach(w => w.write(records.toSeq))
    }
  }

  /**
    * Clean up Cassandra connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Redis sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

}
