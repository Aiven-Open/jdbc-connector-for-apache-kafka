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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A provider of JDBC {@link Connection} instances.
 */
public interface ConnectionProvider extends AutoCloseable {

  /**
   * Create a connection.
   * @return the connection; never null
   * @throws SQLException if there is a problem getting the connection
   */
  Connection getConnection() throws SQLException;

  /**
   * Determine if the specified connection is valid.
   *
   * @param connection the database connection; may not be null
   * @param timeout    The time in seconds to wait for the database operation used to validate
   *                   the connection to complete.  If the timeout period expires before the
   *                   operation completes, this method returns false.  A value of 0 indicates a
   *                   timeout is not applied to the database operation.
   * @return true if it is valid, or false otherwise
   * @throws SQLException if there is an error with the database connection
   */
  boolean isConnectionValid(
      Connection connection,
      int timeout
  ) throws SQLException;

  /**
   * Close this connection provider.
   */
  void close();
}
