package com.datamountaineer.streamreactor.connect.jdbc.sink

import java.util

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}


/**
  * <h1>JdbcSinkConnector</h1>
  * Kafka connect Redis Sink connector
  *
  * Sets up RedisSinkTask and configurations for the tasks.
  **/
class JdbcSinkConnector extends SinkConnector with StrictLogging {
  //???
  private var configProps: util.Map[String, String] = null

  /**
    * States which SinkTask class to use
    **/
  override def taskClass(): Class[_ <: Task] = classOf[JdbcSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(c => configProps).toList.asJava
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Hbase sink task with ${props.toString}.")
    configProps = props
    Try(new JdbcSinkConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start Hbase Sink due to configuration error.", f)
      case _ =>
    }
  }

  override def stop(): Unit = {}

  override def version(): String = getClass.getPackage.getImplementationVersion
}
