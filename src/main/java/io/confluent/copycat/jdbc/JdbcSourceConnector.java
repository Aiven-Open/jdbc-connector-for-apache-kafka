/**
 * Copyright 2015 Confluent Inc.
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

package io.confluent.copycat.jdbc;

import java.util.List;
import java.util.Properties;

import io.confluent.copycat.connector.Connector;
import io.confluent.copycat.connector.Task;
import io.confluent.copycat.errors.CopycatException;

/**
 * JdbcConnector is a Copycat Connector implementation that watches a JDBC database and generates
 * Copycat tasks to ingest database contents.
 */
public class JdbcSourceConnector extends Connector {

  @Override
  public void configure(Properties properties) throws CopycatException {

  }

  @Override
  public Class<? extends Task> getTaskClass() {
    return JdbcSourceTask.class;
  }

  @Override
  public List<Properties> getTaskConfigs(int maxTasks) {
    return null;
  }

  @Override
  public void stop() throws CopycatException {

  }
}
