/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.TableId;

/**
 * Provides helper methods to get partition map for different protocol versions.
 */
public class OffsetProtocols {

    /**
     * Provides the partition map for V1 protocol. The table name included is fully qualified
     * and there is also an explicit protocol key.
     *
     * @param tableId the tableId that requires partition keys
     * @return the partition map for V1 protocol
     */
    public static Map<String, String> sourcePartitionForProtocolV1(final TableId tableId) {
        final String fqn = ExpressionBuilder.create().append(tableId, false).toString();
        final Map<String, String> partitionForV1 = new HashMap<>();
        partitionForV1.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, fqn);
        partitionForV1.put(
            JdbcSourceConnectorConstants.OFFSET_PROTOCOL_VERSION_KEY,
            JdbcSourceConnectorConstants.PROTOCOL_VERSION_ONE
        );
        return partitionForV1;
    }

    /**
     * Provides the partition map for V0 protocol. The table name included is unqualified
     * and there is no explicit protocol key.
     *
     * @param tableId the tableId that requires partition keys
     * @return the partition map for V0 protocol
     */
    public static Map<String, String> sourcePartitionForProtocolV0(final TableId tableId) {
        return Collections.singletonMap(
            JdbcSourceConnectorConstants.TABLE_NAME_KEY,
            tableId.tableName()
        );
    }
}
