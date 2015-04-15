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

import io.confluent.copycat.errors.CopycatException;
import io.confluent.copycat.source.SourceRecord;
import io.confluent.copycat.source.SourceTask;

/**
 * JdbcSourceTask is a Copycat SourceTask implementation that reads from JDBC databases and
 * generates Copycat records.
 */
public class JdbcSourceTask extends SourceTask<Object, Object> {

  @Override
  public void start(Properties properties) {

  }

  @Override
  public void stop() throws CopycatException {

  }

  @Override
  public List<SourceRecord<Object, Object>> poll() throws InterruptedException {
    return null;
  }
}
