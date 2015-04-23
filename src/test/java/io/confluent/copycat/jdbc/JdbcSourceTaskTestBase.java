/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.copycat.jdbc;

import org.junit.After;
import org.junit.Before;

import java.util.Properties;

import io.confluent.common.utils.MockTime;
import io.confluent.common.utils.Time;

public class JdbcSourceTaskTestBase {

  protected static String SINGLE_TABLE_NAME = "test";
  protected static EmbeddedDerby.TableName SINGLE_TABLE
      = new EmbeddedDerby.TableName(SINGLE_TABLE_NAME);

  protected Time time;
  protected JdbcSourceTask task;
  protected EmbeddedDerby db;

  @Before
  public void setup() throws Exception {
    time = new MockTime();
    task = new JdbcSourceTask(time);
    db = new EmbeddedDerby();
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  protected Properties singleTableConfig() {
    Properties props = new Properties();
    props.setProperty(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.setProperty(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME);
    return props;
  }
}
