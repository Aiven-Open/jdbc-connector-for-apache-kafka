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

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Connection pooling helper.
 */
public final class HikariHelper {

  /**
   * Creates an instance of HikariDataSource from the jdbc sink settings
   *
   * @param dbUri    - THe database connection string
   * @param user     - The database user
   * @param password - The database password
   * @return
   */
  public final static HikariDataSource from(final String dbUri,
                                            final String user,
                                            final String password) {
    ParameterValidator.notNullOrEmpty(dbUri, "dbUri");
    final HikariConfig config = new HikariConfig();
    config.setJdbcUrl(dbUri);

    if (user != null && user.trim().length() > 0) {
      config.setUsername(user);
    }

    if (password != null && password.trim().length() > 0) {
      config.setPassword(password);
    }
    return new HikariDataSource(config);
  }
}
