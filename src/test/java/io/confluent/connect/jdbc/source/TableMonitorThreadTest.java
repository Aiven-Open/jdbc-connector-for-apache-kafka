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

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.JdbcUtils;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TableMonitorThread.class, JdbcUtils.class})
@PowerMockIgnore("javax.management.*")
public class TableMonitorThreadTest {
  private static final long POLL_INTERVAL = 100;

  private static final List<String> FIRST_TOPIC_LIST = Arrays.asList("foo");
  private static final List<String> VIEW_TOPIC_LIST = Arrays.asList("");
  private static final List<String> SECOND_TOPIC_LIST = Arrays.asList("foo", "bar");
  private static final List<String> THIRD_TOPIC_LIST = Arrays.asList("foo", "bar", "baz");
  public static final Set<String> DEFAULT_TABLE_TYPES = Collections.unmodifiableSet(
          new HashSet<String>(Arrays.asList("TABLE"))
  );
  public static final Set<String> VIEW_TABLE_TYPES = Collections.unmodifiableSet(
          new HashSet<String>(Arrays.asList("VIEW"))
  );
  private EmbeddedDerby db;
  private CachedConnectionProvider cachedConnectionProvider;
  private TableMonitorThread tableMonitorThread;
  @Mock private ConnectorContext context;

  @Before
  public void setUp() throws SQLException {
    db = new EmbeddedDerby();
    cachedConnectionProvider = new CachedConnectionProvider(db.getUrl());

    PowerMock.mockStatic(JdbcUtils.class);
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testSingleLookup() throws Exception {
    tableMonitorThread = new TableMonitorThread(cachedConnectionProvider, context, null, POLL_INTERVAL, null, null, DEFAULT_TABLE_TYPES);

    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, DEFAULT_TABLE_TYPES)).andAnswer(new IAnswer<List<String>>() {
      @Override
      public List<String> answer() throws Throwable {
        tableMonitorThread.shutdown();
        return FIRST_TOPIC_LIST;
      }
    });

    PowerMock.replayAll();

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertEquals(FIRST_TOPIC_LIST, tableMonitorThread.tables());

    PowerMock.verifyAll();
  }

  @Test
  public void testWhitelist() throws Exception {
    tableMonitorThread = new TableMonitorThread(cachedConnectionProvider, context, null, POLL_INTERVAL,
                                                new HashSet<>(Arrays.asList("foo", "bar")), null, DEFAULT_TABLE_TYPES);

    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, DEFAULT_TABLE_TYPES)).andAnswer(new IAnswer<List<String>>() {
      @Override
      public List<String> answer() throws Throwable {
        tableMonitorThread.shutdown();
        return THIRD_TOPIC_LIST;
      }
    });

    PowerMock.replayAll();

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertEquals(Arrays.asList("foo", "bar"), tableMonitorThread.tables());

    PowerMock.verifyAll();
  }

  @Test
  public void testBlacklist() throws Exception {
    tableMonitorThread = new TableMonitorThread(cachedConnectionProvider, context, null, POLL_INTERVAL,
                                                null, new HashSet<>(Arrays.asList("bar", "baz")), DEFAULT_TABLE_TYPES);

    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, DEFAULT_TABLE_TYPES)).andAnswer(new IAnswer<List<String>>() {
      @Override
      public List<String> answer() throws Throwable {
        tableMonitorThread.shutdown();
        return THIRD_TOPIC_LIST;
      }
    });

    PowerMock.replayAll();

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertEquals(Arrays.asList("foo"), tableMonitorThread.tables());

    PowerMock.verifyAll();
  }

  @Test
  public void testReconfigOnUpdate() throws Exception {
    tableMonitorThread = new TableMonitorThread(cachedConnectionProvider, context, null, POLL_INTERVAL, null, null, DEFAULT_TABLE_TYPES);

    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, DEFAULT_TABLE_TYPES)).andReturn(FIRST_TOPIC_LIST);
    // Returning same list should not change results
    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, DEFAULT_TABLE_TYPES)).andAnswer(new IAnswer<List<String>>() {
      @Override
      public List<String> answer() throws Throwable {
        assertEquals(FIRST_TOPIC_LIST, tableMonitorThread.tables());
        return FIRST_TOPIC_LIST;
      }
    });
    // Changing the result should trigger a task reconfiguration
    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, DEFAULT_TABLE_TYPES)).andReturn(SECOND_TOPIC_LIST);
    context.requestTaskReconfiguration();
    PowerMock.expectLastCall();
    // Changing again should result in another update
    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, DEFAULT_TABLE_TYPES)).andAnswer(new IAnswer<List<String>>() {
      @Override
      public List<String> answer() throws Throwable {
        assertEquals(SECOND_TOPIC_LIST, tableMonitorThread.tables());
        tableMonitorThread.shutdown();
        return FIRST_TOPIC_LIST;
      }
    });
    context.requestTaskReconfiguration();
    PowerMock.expectLastCall();

    PowerMock.replayAll();

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertEquals(FIRST_TOPIC_LIST, tableMonitorThread.tables());

    PowerMock.verifyAll();
  }

  @Test
  public void testTableType() throws Exception {
    tableMonitorThread = new TableMonitorThread(cachedConnectionProvider, context, null, POLL_INTERVAL, null, null, VIEW_TABLE_TYPES);

    EasyMock.expect(JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), null, VIEW_TABLE_TYPES)).andAnswer(new IAnswer<List<String>>() {
      @Override
      public List<String> answer() throws Throwable {
        tableMonitorThread.shutdown();
        return VIEW_TOPIC_LIST;
      }
    });

    PowerMock.replayAll();

    tableMonitorThread.start();
    tableMonitorThread.join();


    assertEquals(VIEW_TOPIC_LIST, tableMonitorThread.tables());

    PowerMock.verifyAll();
  }
}
