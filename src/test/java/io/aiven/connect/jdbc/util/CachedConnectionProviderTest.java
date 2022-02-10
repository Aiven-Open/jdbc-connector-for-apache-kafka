/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2017 Confluent Inc.
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

package io.aiven.connect.jdbc.util;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.kafka.connect.errors.ConnectException;

import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertNotNull;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CachedConnectionProviderTest.class})
@PowerMockIgnore("javax.management.*")
public class CachedConnectionProviderTest {

    @Mock
    private ConnectionProvider provider;

    @Test
    public void retryTillFailure() throws SQLException {
        final int retries = 15;
        final ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
        EasyMock.expect(provider.getConnection()).andThrow(new SQLException("test")).times(retries);
        PowerMock.replayAll();

        try {
            connectionProvider.getConnection();
        } catch (final ConnectException ex) {
            assertNotNull(ex);
        }

        PowerMock.verifyAll();
    }


    @Test
    public void retryTillConnect() throws SQLException {
        final Connection connection = EasyMock.createMock(Connection.class);
        final int retries = 15;

        final ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
        EasyMock.expect(provider.getConnection())
            .andThrow(new SQLException("test"))
            .times(retries - 1)
            .andReturn(connection);
        PowerMock.replayAll();

        assertNotNull(connectionProvider.getConnection());

        PowerMock.verifyAll();
    }

}
