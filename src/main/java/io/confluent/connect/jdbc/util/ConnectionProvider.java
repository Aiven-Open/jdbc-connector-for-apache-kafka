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
   * Close this connection provider.
   */
  void close();
}
