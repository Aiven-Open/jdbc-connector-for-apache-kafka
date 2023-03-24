/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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
 */

package io.aiven.connect.jdbc.source;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.util.ConnectionProvider;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.TableId;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({JdbcSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class TableMonitorThreadTest {
    private static final long POLL_INTERVAL = 100;

    private static final TableId FOO = new TableId(null, null, "foo");
    private static final TableId BAR = new TableId(null, null, "bar");
    private static final TableId BAZ = new TableId(null, null, "baz");
    private static final TableId QUAL = new TableId("fully", "qualified", "name");

    private static final TableId DUP1 = new TableId(null, "dup1", "dup");
    private static final TableId DUP2 = new TableId(null, "dup2", "dup");

    private static final List<TableId> LIST_EMPTY = Collections.emptyList();
    private static final List<TableId> LIST_FOO = Collections.singletonList(FOO);
    private static final List<TableId> LIST_FOO_BAR = Arrays.asList(FOO, BAR);
    private static final List<TableId> LIST_FOO_BAR_BAZ = Arrays.asList(FOO, BAR, BAZ);
    private static final List<TableId> LIST_DUP_ONLY = Arrays.asList(DUP1, DUP2);
    private static final List<TableId> LIST_DUP_WITH_ALL = Arrays.asList(DUP1, FOO, DUP2, BAR, BAZ);

    private static final List<String> FIRST_TOPIC_LIST = Arrays.asList("foo");
    private static final List<String> VIEW_TOPIC_LIST = Arrays.asList("");
    private static final List<String> SECOND_TOPIC_LIST = Arrays.asList("foo", "bar");
    private static final List<String> THIRD_TOPIC_LIST = Arrays.asList("foo", "bar", "baz");
    public static final Set<String> DEFAULT_TABLE_TYPES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("TABLE"))
    );
    public static final Set<String> VIEW_TABLE_TYPES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("VIEW"))
    );

    private final boolean qualifiedTableNames;

    private TableMonitorThread tableMonitorThread;

    @Mock
    private ConnectionProvider connectionProvider;
    @Mock
    private Connection connection;
    @Mock
    private DatabaseDialect dialect;
    @Mock
    private ConnectorContext context;

    @Parameterized.Parameters
    public static Iterable<? extends Object> data() {
        return Arrays.asList(false, true);
    }

    public TableMonitorThreadTest(final boolean qualifiedTableNames) {
        this.qualifiedTableNames = qualifiedTableNames;
    }

    @Test
    public void testSingleLookup() throws Exception {
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(null, null);
        final String expectedTableName;
        if (qualifiedTableNames) {
            expectTableNames(LIST_FOO, shutdownThread());
            expectedTableName = "foo";
        } else {
            expectTableNames(Collections.singletonList(QUAL), shutdownThread());
            expectedTableName = "name";
        }
        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();
        checkTableNames(expectedTableName).execute();

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testWhitelist() throws Exception {
        final Set<String> whitelist = new HashSet<>(Arrays.asList("foo", "bar"));
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(whitelist, null);
        expectTableNames(LIST_FOO_BAR, shutdownThread());
        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();
        checkTableNames("foo", "bar").execute();

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testBlacklist() throws Exception {
        final Set<String> blacklist = new HashSet<>(Arrays.asList("bar", "baz"));
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(null, blacklist);
        expectTableNames(LIST_FOO_BAR_BAZ, shutdownThread());
        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();
        checkTableNames("foo").execute();

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testReconfigOnUpdate() throws Exception {
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(null, null);
        expectTableNames(LIST_FOO);
        expectTableNames(LIST_FOO, checkTableNames("foo"));

        // Change the result to trigger a task reconfiguration
        expectTableNames(LIST_FOO_BAR);
        context.requestTaskReconfiguration();
        EasyMock.expectLastCall();

        // Changing again should result in another task reconfiguration
        expectTableNames(LIST_FOO, checkTableNames("foo", "bar"), shutdownThread());
        context.requestTaskReconfiguration();
        EasyMock.expectLastCall();

        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();
        checkTableNames("foo").execute();

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testInvalidConnection() throws Exception {
        tableMonitorThread = newTableMonitorThread(null, null);
        EasyMock.expect(connectionProvider.getConnection()).andAnswer(new IAnswer<Connection>() {
            @Override
            public Connection answer() throws Throwable {
                tableMonitorThread.shutdown();
                throw new ConnectException("Simulated error with the db.");
            }
        });
        connectionProvider.close();
        EasyMock.expectLastCall().anyTimes();

        EasyMock.replay(connectionProvider);

        tableMonitorThread.start();
        tableMonitorThread.join();

        EasyMock.verify(connectionProvider);
    }

    @Test
    public void testDuplicates() throws Exception {
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(null, null);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
        EasyMock.replay(connectionProvider, dialect);
        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            assertThrows(ConnectException.class, tableMonitorThread::tables);
        } else {
            checkTableNames("foo", "bar", "baz", "dup");
        }

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testDuplicateWithUnqualifiedWhitelist() throws Exception {
        final Set<String> whitelist = new HashSet<>(Arrays.asList("dup"));
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(whitelist, null);
        expectTableNames(LIST_DUP_ONLY, shutdownThread());
        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            assertThrows(ConnectException.class, tableMonitorThread::tables);
        } else {
            checkTableNames("dup");
        }

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testDuplicateWithUnqualifiedBlacklist() throws Exception {
        final Set<String> blacklist = new HashSet<>(Arrays.asList("foo"));
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(null, blacklist);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            assertThrows(ConnectException.class, tableMonitorThread::tables);
        } else {
            checkTableNames("bar", "baz", "dup");
        }

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testDuplicateWithQualifiedWhitelist() throws Exception {
        final Set<String> whitelist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(whitelist, null);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            checkTableIds(DUP1, FOO);
        } else {
            checkTableNames("dup", "foo");
        }

        EasyMock.verify(connectionProvider, dialect);
    }

    @Test
    public void testDuplicateWithQualifiedBlacklist() throws Exception {
        final Set<String> blacklist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
        EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
        tableMonitorThread = newTableMonitorThread(null, blacklist);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
        EasyMock.replay(connectionProvider, dialect);

        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            checkTableIds(DUP2, BAR, BAZ);
        } else {
            checkTableNames("dup", "bar", "baz");
        }
        EasyMock.verify(connectionProvider, dialect);
    }

    private TableMonitorThread newTableMonitorThread(final Set<String> whitelist, final Set<String> blacklist) {
        return new TableMonitorThread(
                dialect,
                connectionProvider,
                context,
                POLL_INTERVAL,
                whitelist,
                blacklist,
                qualifiedTableNames
        );
    }

    private interface Op {
        void execute();
    }

    protected Op shutdownThread() {
        return new Op() {
            @Override
            public void execute() {
                tableMonitorThread.shutdown();
            }
        };
    }

    protected Op checkTableNames(final String... expectedTableNames) {
        return new Op() {
            @Override
            public void execute() {
                final List<TableId> expectedTableIds = new ArrayList<>();
                for (final String expectedTableName : expectedTableNames) {
                    final TableId id = new TableId(null, null, expectedTableName);
                    expectedTableIds.add(id);
                }
                assertEquals(expectedTableIds, tableMonitorThread.tables());
            }
        };
    }

    protected void checkTableIds(final TableId... expectedTables) {
        assertEquals(Arrays.asList(expectedTables), tableMonitorThread.tables());
    }

    protected void expectTableNames(final List<TableId> expectedTableIds, final Op... operations)
        throws
        SQLException {
        EasyMock.expect(connectionProvider.getConnection()).andReturn(connection);
        EasyMock.expect(dialect.tableIds(EasyMock.eq(connection))).andAnswer(
            new IAnswer<List<TableId>>() {
                @Override
                public List<TableId> answer() throws Throwable {
                    if (operations != null) {
                        for (final Op op : operations) {
                            op.execute();
                        }
                    }
                    return expectedTableIds;
                }
            });
    }
}
