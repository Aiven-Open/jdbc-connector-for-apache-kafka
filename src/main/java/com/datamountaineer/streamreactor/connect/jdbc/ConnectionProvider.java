/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


package com.datamountaineer.streamreactor.connect.jdbc;

import com.datamountaineer.streamreactor.connect.jdbc.common.ParameterValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Responsible for creating the database connection. It provides automatic retries.
 */
public class ConnectionProvider {
  private final Logger logger = LoggerFactory.getLogger(ConnectionProvider.class);

  private final int retries;
  private final int delayBetweenRetries;
  private final String uri;
  private final String user;
  private final String password;

  public ConnectionProvider(String uri, String user, String password, int retries, int delayBetweenRetries) {
    ParameterValidator.notNullOrEmpty(uri, "uri");
    this.retries = retries;
    this.delayBetweenRetries = delayBetweenRetries;
    this.uri = uri;
    this.user = user;
    this.password = password;
  }

  /**
   * Returns a new instance of jdbc Connection
   *
   * @return
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    Connection connection = null;
    int retriesLeft = retries;
    SQLException exception = null;
    while (retriesLeft > 0 && connection == null) {
      try {
        connection = DriverManager.getConnection(uri, user, password);
      } catch (SQLException e) {
        retriesLeft--;
        if (retriesLeft == 0) {
          exception = e;
        }
        logger.warn(String.format("Trying to open the database failed. Retries left %d", retriesLeft), e);
        try {
          Thread.sleep(delayBetweenRetries);
        } catch (InterruptedException e1) {

        }
      }
    }
    if (exception != null) {
      throw exception;
    }

    return connection;
  }

}
