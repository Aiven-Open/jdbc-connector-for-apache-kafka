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
import java.time.Duration;
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
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

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSingleLookup() throws Exception {
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(null, null);
        final String expectedTableName;
        if (qualifiedTableNames) {
            expectTableNames(LIST_FOO, shutdownThread());
            expectedTableName = "foo";
        } else {
            expectTableNames(Collections.singletonList(QUAL), shutdownThread());
            expectedTableName = "name";
        }
        tableMonitorThread.start();
        tableMonitorThread.join();
        checkTableNames(expectedTableName).execute();

        verify(dialect).expressionBuilder();
    }

    @Test
    public void testWhitelist() throws Exception {
        final Set<String> whitelist = new HashSet<>(Arrays.asList("foo", "bar"));
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(whitelist, null);
        expectTableNames(LIST_FOO_BAR, shutdownThread());

        tableMonitorThread.start();
        tableMonitorThread.join();
        checkTableNames("foo", "bar").execute();

        verify(dialect, atLeastOnce()).expressionBuilder();
    }

    @Test
    public void testBlacklist() throws Exception {
        final Set<String> blacklist = new HashSet<>(Arrays.asList("bar", "baz"));
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(null, blacklist);
        expectTableNames(LIST_FOO_BAR_BAZ, shutdownThread());

        tableMonitorThread.start();
        tableMonitorThread.join();
        checkTableNames("foo").execute();

        verify(dialect, atLeastOnce()).expressionBuilder();
    }

    @Test
    public void testReconfigOnUpdate() throws Exception {
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(null, null);
        expectTableNames(LIST_FOO);
        expectTableNames(LIST_FOO, checkTableNames("foo"));

        // Change the result to trigger a task reconfiguration
        expectTableNames(LIST_FOO_BAR);

        tableMonitorThread.start();

        await().atMost(Duration.ofMillis(1000)).pollInterval(Duration.ofMillis(100))
            .until(() -> !Mockito.mockingDetails(dialect).getInvocations().isEmpty());
        expectTableNames(LIST_FOO, checkTableNames("foo", "bar"), shutdownThread());
        tableMonitorThread.join();
        checkTableNames("foo").execute();
        verify(context, atLeastOnce()).requestTaskReconfiguration();
        verify(dialect, times(2)).expressionBuilder();
    }

    @Test
    public void testInvalidConnection() throws Exception {
        tableMonitorThread = newTableMonitorThread(null, null);
        when(connectionProvider.getConnection()).thenAnswer(new Answer<Connection>() {
            @Override
            public Connection answer(final InvocationOnMock invocation) throws Throwable {
                tableMonitorThread.shutdown();
                throw new ConnectException("Simulated error with the db.");
            }
        });

        tableMonitorThread.start();
        tableMonitorThread.join();

        verify(connectionProvider).getConnection();
    }

    @Test
    public void testDuplicates() throws Exception {
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(null, null);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            assertThatThrownBy(tableMonitorThread::tables).isInstanceOf(ConnectException.class);
        } else {
            checkTableNames("foo", "bar", "baz", "dup");
        }

        verify(dialect).expressionBuilder();
    }

    @Test
    public void testDuplicateWithUnqualifiedWhitelist() throws Exception {
        final Set<String> whitelist = new HashSet<>(Arrays.asList("dup"));
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(whitelist, null);
        expectTableNames(LIST_DUP_ONLY, shutdownThread());

        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            assertThatThrownBy(tableMonitorThread::tables).isInstanceOf(ConnectException.class);
        } else {
            checkTableNames("dup");
        }

        verify(dialect, atLeastOnce()).expressionBuilder();
    }

    @Test
    public void testDuplicateWithUnqualifiedBlacklist() throws Exception {
        final Set<String> blacklist = new HashSet<>(Arrays.asList("foo"));
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(null, blacklist);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            assertThatThrownBy(tableMonitorThread::tables).isInstanceOf(ConnectException.class);
        } else {
            checkTableNames("bar", "baz", "dup");
        }

        verify(dialect, atLeastOnce()).expressionBuilder();
    }

    @Test
    public void testDuplicateWithQualifiedWhitelist() throws Exception {
        final Set<String> whitelist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(whitelist, null);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());

        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            checkTableIds(DUP1, FOO);
        } else {
            checkTableNames("dup", "foo");
        }

        verify(dialect, atLeastOnce()).expressionBuilder();
    }

    @Test
    public void testDuplicateWithQualifiedBlacklist() throws Exception {
        final Set<String> blacklist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
        when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
        tableMonitorThread = newTableMonitorThread(null, blacklist);
        expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());

        tableMonitorThread.start();
        tableMonitorThread.join();

        if (qualifiedTableNames) {
            checkTableIds(DUP2, BAR, BAZ);
        } else {
            checkTableNames("dup", "bar", "baz");
        }
        verify(dialect, atLeastOnce()).expressionBuilder();
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
                assertThat(tableMonitorThread.tables()).isEqualTo(expectedTableIds);
            }
        };
    }

    protected void checkTableIds(final TableId... expectedTables) {
        assertThat(tableMonitorThread.tables()).containsExactly(expectedTables);
    }

    protected void expectTableNames(final List<TableId> expectedTableIds, final Op... operations)
        throws
        SQLException {
        when(connectionProvider.getConnection()).thenReturn(connection);
        when(dialect.tableIds(eq(connection))).thenAnswer(
            invocation -> {
                if (operations != null) {
                    for (final Op op : operations) {
                        op.execute();
                    }
                }
                return expectedTableIds;
            });
    }
}
