/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

import io.aiven.connect.jdbc.util.CachedConnectionProvider;
import io.aiven.connect.jdbc.util.ConnectionProvider;

class SourceConnectionProvider extends CachedConnectionProvider {
    SourceConnectionProvider(
        final ConnectionProvider provider,
        final int maxConnectionAttempts,
        final long connectionRetryBackoff
    ) {
        super(provider, maxConnectionAttempts, connectionRetryBackoff);
    }

    @Override
    protected void onConnect(final Connection connection) throws SQLException {
        super.onConnect(connection);
        connection.setAutoCommit(false);
    }
}
