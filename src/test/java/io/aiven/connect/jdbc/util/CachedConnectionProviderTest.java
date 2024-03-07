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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CachedConnectionProviderTest {

    @Mock
    private ConnectionProvider provider;

    @Test
    public void retryTillFailure() throws SQLException {
        final int retries = 15;
        final ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
        when(provider.getConnection()).thenThrow(new SQLException("test"));

        assertThatThrownBy(connectionProvider::getConnection)
            .isInstanceOf(ConnectException.class);

        verify(provider, times(retries)).getConnection();
    }


    @Test
    public void retryTillConnect() throws SQLException {
        final Connection connection = mock(Connection.class);
        final int retries = 4;

        final ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
        when(provider.getConnection())
            .thenThrow(new SQLException("test"))
            .thenThrow(new SQLException("test"))
            .thenThrow(new SQLException("test"))
            .thenReturn(connection);

        assertThat(connectionProvider.getConnection()).isNotNull();
        verify(provider, times(4)).getConnection();
    }

}
