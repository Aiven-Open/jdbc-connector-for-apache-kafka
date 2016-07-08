package io.confluent.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import io.confluent.connect.jdbc.sink.common.ParameterValidator;

public class ConnectionProvider {

  private final String uri;
  private final String user;
  private final String password;

  public ConnectionProvider(String uri, String user, String password) {
    ParameterValidator.notNullOrEmpty(uri, "uri");
    this.uri = uri;
    this.user = user;
    this.password = password;
  }

  public Connection getConnection() throws SQLException {
    return DriverManager.getConnection(uri, user, password);
  }

}
