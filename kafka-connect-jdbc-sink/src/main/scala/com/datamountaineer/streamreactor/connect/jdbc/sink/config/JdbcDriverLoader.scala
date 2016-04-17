package com.datamountaineer.streamreactor.connect.jdbc.sink.config

import java.io.File
import java.net.URLClassLoader
import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger

import com.datamountaineer.streamreactor.connect.Enumerators
import com.typesafe.scalalogging.slf4j.StrictLogging

/**
  * Responsible for loading the given jdbc driver manager
  */
object JdbcDriverLoader extends StrictLogging {
  def apply(driver: String, jar: File): Boolean = {
    require(jar != null && jar.exists())
    if (Enumerators(DriverManager.getDrivers)
      .collectFirst { case dw if dw.getClass == classOf[DriverWrapper] && dw.asInstanceOf[DriverWrapper].driver.getClass.getCanonicalName == driver => dw}.isEmpty) {
      logger.debug(s"Loading Jdbc driver: $driver")
      val ucl = new URLClassLoader(Array(jar.toURL), classOf[System].getClassLoader)

      val d = Class.forName(driver, true, ucl).newInstance().asInstanceOf[Driver]
      DriverManager.registerDriver(DriverWrapper(d))

      logger.debug("$driver has been loaded")
      true
    }
    else false
  }

  case class DriverWrapper(driver: Driver) extends Driver {
    override def acceptsURL(s: String): Boolean = driver.acceptsURL(s)

    override def jdbcCompliant(): Boolean = driver.jdbcCompliant()

    override def getPropertyInfo(s: String, properties: Properties): Array[DriverPropertyInfo] = driver.getPropertyInfo(s, properties)

    override def getMinorVersion: Int = driver.getMinorVersion

    override def getParentLogger: Logger = driver.getParentLogger

    override def connect(s: String, properties: Properties): Connection = driver.connect(s, properties)

    override def getMajorVersion: Int = driver.getMajorVersion
  }

}
