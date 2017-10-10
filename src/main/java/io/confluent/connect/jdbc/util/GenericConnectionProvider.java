/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A {@link ConnectionProvider} that uses the supplied JDBC URL and connection properties. This
 * implementation does not cache the {@link Connection}; see {@link CachedConnectionProvider} for a
 * delegating implementation that does.
 */
public class GenericConnectionProvider implements ConnectionProvider {

  private static final Logger log = LoggerFactory.getLogger(GenericConnectionProvider.class);

  private final String jdbcUrl;
  private final Properties properties;

  public GenericConnectionProvider(
      String jdbcUrl,
      Properties properties
  ) {
    this.jdbcUrl = jdbcUrl;
    this.properties = properties;
  }

  public GenericConnectionProvider(
      String jdbcUrl,
      String user,
      String password
  ) {
    this.jdbcUrl = jdbcUrl;
    this.properties = new Properties();
    if (user != null) {
      this.properties.setProperty("user", user);
    }
    if (password != null) {
      this.properties.setProperty("password", password);
    }
  }

  @Override
  public synchronized Connection getConnection() throws SQLException {
    return DriverManager.getConnection(jdbcUrl, properties);
  }

  @Override
  public synchronized void close() {
  }

  @Override
  public String toString() {
    return jdbcUrl;
  }
}
