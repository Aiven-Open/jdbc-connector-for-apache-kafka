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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import io.confluent.copycat.errors.CopycatRuntimeException;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceTaskLifecycleTest extends JdbcSourceTaskTestBase {

  @Test(expected = CopycatRuntimeException.class)
  public void testMissingParentConfig() {
    Properties props = singleTableConfig();
    props.remove(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    task.start(props);
  }

  @Test(expected = CopycatRuntimeException.class)
  public void testMissingTables() {
    Properties props = singleTableConfig();
    props.remove(JdbcSourceTaskConfig.TABLES_CONFIG);
    task.start(props);
  }

  @Test
  public void testStartStop() throws Exception {
    // Minimal start/stop functionality
    PowerMock.mockStatic(DriverManager.class);

    // Should request a connection, then should close it on stop()
    Connection conn = PowerMock.createMock(Connection.class);
    EasyMock.expect(DriverManager.getConnection(db.getUrl()))
        .andReturn(conn);
    conn.close();
    PowerMock.expectLastCall();

    PowerMock.replayAll();

    task.start(singleTableConfig());
    task.stop();

    PowerMock.verifyAll();
  }

  @Test
  public void testPollInterval() throws Exception {
    // Here we just want to verify behavior of the poll method, not any loading of data, so we
    // specifically want an empty
    db.createTable(SINGLE_TABLE_NAME, "id", "INT");

    long startTime = time.milliseconds();
    task.start(singleTableConfig());

    // First poll should happen immediately
    task.poll();
    assertEquals(startTime, time.milliseconds());

    // Subsequent polls have to wait for timeout
    task.poll();
    assertEquals(startTime + JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
                 time.milliseconds());
    task.poll();
    assertEquals(startTime + 2 * JdbcSourceConnectorConfig.POLL_INTERVAL_MS_DEFAULT,
                 time.milliseconds());

    task.stop();
  }

}
