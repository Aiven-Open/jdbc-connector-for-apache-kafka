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
        logger.warn("Trying to open the database failed. {} retries left", retriesLeft, e);
        try {
          Thread.sleep(delayBetweenRetries);
        } catch (InterruptedException ie) {
          throw new SQLException(ie);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }

    return connection;
  }

}
