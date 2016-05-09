/**
 * Copyright 2015 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Connection;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Properties;

/**
 * Responsible for loading the given jdbc driver manager
 */
public final class JdbcDriverLoader {
  private static final Logger logger = LoggerFactory.getLogger(JdbcDriverLoader.class);

  public static boolean load(final String driver, final File jar) {
    if (jar == null || !jar.exists()) {
      throw new IllegalArgumentException("Invalid jar file." + (jar != null ? jar.getAbsolutePath() : null));
    }

    final Enumeration<Driver> drivers = DriverManager.getDrivers();

    final Driver foundDriver = Iterators.find(Iterators.forEnumeration(drivers), new Predicate<Driver>() {
        @Override
        public boolean apply(Driver input) {
          return input.getClass() == DriverWrapper.class &&
                  Objects.equals(((DriverWrapper) input).driver.getClass().getCanonicalName(), driver);
        }
    }, null);

    if (foundDriver == null) {
      logger.debug("Loading Jdbc driver: " + driver);

      try {
        final URLClassLoader ucl = new URLClassLoader(new URL[]{jar.toURI().toURL()});

        final Driver newDriver = (Driver) Class.forName(driver, true, ucl).newInstance();
        DriverManager.registerDriver(new DriverWrapper(newDriver));

        logger.debug("$driver has been loaded");
        return true;
      } catch (Throwable t) {
        logger.error("Couldn't load " + driver, t);
        return false;
      }
    } else {
      logger.debug("Ther driver " + driver + " is already loaded.");
      return false;
    }
  }

  private static final class DriverWrapper implements Driver {
    private final Driver driver;

    DriverWrapper(Driver driver) {
      this.driver = driver;
    }

    @Override
    public boolean acceptsURL(String s) throws SQLException {
      return driver.acceptsURL(s);
    }

    @Override
    public boolean jdbcCompliant() {
      return driver.jdbcCompliant();
    }


    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
      return driver.getPropertyInfo(s, properties);
    }

    @Override
    public int getMinorVersion() {
      return driver.getMinorVersion();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return driver.getParentLogger();
    }

    @Override
    public Connection connect(String s, Properties properties) throws SQLException {
      return driver.connect(s, properties);
    }

    @Override
    public int getMajorVersion() {
      return driver.getMajorVersion();
    }
  }

}
