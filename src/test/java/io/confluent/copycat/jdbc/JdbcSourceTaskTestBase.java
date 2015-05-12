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

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.powermock.api.easymock.PowerMock;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.common.utils.MockTime;
import io.confluent.common.utils.Time;
import io.confluent.copycat.data.Schema;
import io.confluent.copycat.data.SchemaBuilder;
import io.confluent.copycat.source.SourceTaskContext;
import io.confluent.copycat.storage.OffsetStorageReader;

public class JdbcSourceTaskTestBase {

  protected static String SINGLE_TABLE_NAME = "test";
  protected static EmbeddedDerby.TableName SINGLE_TABLE
      = new EmbeddedDerby.TableName(SINGLE_TABLE_NAME);

  protected static String SECOND_TABLE_NAME = "test2";
  protected static EmbeddedDerby.TableName SECOND_TABLE
      = new EmbeddedDerby.TableName(SECOND_TABLE_NAME);

  protected static Schema offsetSchema = SchemaBuilder.builder().longType();

  protected Time time;
  protected SourceTaskContext taskContext;
  protected JdbcSourceTask task;
  protected EmbeddedDerby db;

  @Before
  public void setup() throws Exception {
    time = new MockTime();
    task = new JdbcSourceTask(time);
    taskContext = PowerMock.createMock(SourceTaskContext.class);
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

  protected Properties twoTableConfig() {
    Properties props = new Properties();
    props.setProperty(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.setProperty(JdbcSourceTaskConfig.TABLES_CONFIG,
                      SINGLE_TABLE_NAME + "," + SECOND_TABLE_NAME);
    return props;
  }

  protected void expectInitialize(Collection<Object> streams, Map<Object, Object> offsets) {
    OffsetStorageReader reader = PowerMock.createMock(OffsetStorageReader.class);
    EasyMock.expect(taskContext.getOffsetStorageReader()).andReturn(reader);
    EasyMock.expect(reader.getOffsets(EasyMock.eq(streams), EasyMock.eq(offsetSchema)))
        .andReturn(offsets);
  }

  protected void expectInitializeNoOffsets(Collection<Object> streams) {
    HashMap<Object, Object> offsets = new HashMap<Object, Object>();
    for(Object stream : streams) {
      offsets.put(stream, null);
    }
    expectInitialize(streams, offsets);
  }

  protected void initializeTask() {
    task.initialize(taskContext);
  }

}
