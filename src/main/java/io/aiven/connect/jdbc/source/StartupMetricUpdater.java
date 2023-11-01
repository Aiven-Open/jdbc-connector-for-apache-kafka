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

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.util.CachedConnectionProvider;
import io.aiven.connect.jdbc.util.StartupMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.connect.jdbc.source.JdbcSourceTaskConfig.TABLES_CONFIG;

public class StartupMetricUpdater {

    private static final Logger log = LoggerFactory.getLogger(StartupMetricUpdater.class);

    private final DatabaseDialect dialect;
    private final CachedConnectionProvider cachedConnectionProvider;
    private final JdbcSourceConnectorConfig config;

    Map<String, StartupMetric> startupMetricByQueryOrTable;

    Map<String, CountQuerier> countQuerierByTableOrQuery;

    public StartupMetricUpdater(final DatabaseDialect dialect, final CachedConnectionProvider cachedConnectionProvider,
                                final JdbcSourceConnectorConfig config) {
        this.dialect = dialect;
        this.cachedConnectionProvider = cachedConnectionProvider;
        this.config = config;
        startupMetricByQueryOrTable = new HashMap<>();
        countQuerierByTableOrQuery = new HashMap<>();
    }

    public void initializeAndExecuteMetric(final Map<String, String> properties, final String query) {
        final CountQuerier.QueryMode queryMode = !query.isEmpty()
                ? CountQuerier.QueryMode.QUERY : CountQuerier.QueryMode.TABLE;
        final List<String> tablesOrQuery = queryMode == CountQuerier.QueryMode.QUERY
                ? Collections.singletonList(query) : config.getList(TABLES_CONFIG);

        final String taskName = properties.get("name");

        for (final String tableOrQuery : tablesOrQuery) {
            getOrCreateStartupMetric(taskName, queryMode, tableOrQuery);
            getOrCreateQuryCounter(queryMode, tableOrQuery);
            updateMetric(tableOrQuery);
        }
    }

    private CountQuerier getOrCreateQuryCounter(final CountQuerier.QueryMode queryMode, final String tableOrQuery) {
        if (countQuerierByTableOrQuery.containsKey(tableOrQuery)) {
            return countQuerierByTableOrQuery.get(tableOrQuery);
        }
        final CountQuerier countQuerier = new CountQuerier(dialect, queryMode, tableOrQuery);
        countQuerierByTableOrQuery.put(tableOrQuery, countQuerier);
        return countQuerier;
    }

    public Long updateMetric(final String tableOrQuery) {
        final StartupMetric metricsProvider = startupMetricByQueryOrTable.get(tableOrQuery);
        final CountQuerier countQuerier = countQuerierByTableOrQuery.get(tableOrQuery);
        try {
            countQuerier.getOrCreatePreparedStatement(cachedConnectionProvider.getConnection());
            final Long counter = countQuerier.count();
            metricsProvider.updateCounter(counter);
            log.info("Update StartupMetric for {}. Set Counter to {}",
                    tableOrQuery.substring(0, Math.min(10, tableOrQuery.length())), counter);
            return counter;
        } catch (final SQLException e) {
            log.error("Exception while querying for number of possible source records", e);
        }
        return -1L;
    }

    private StartupMetric getOrCreateStartupMetric(final String taskName,
                                                   final CountQuerier.QueryMode queryMode, final String tableOrQuery) {
        if (startupMetricByQueryOrTable.containsKey(tableOrQuery)) {
            return startupMetricByQueryOrTable.get(tableOrQuery);
        }

        final StartupMetric metricsProvider = new StartupMetric();
        startupMetricByQueryOrTable.put(tableOrQuery, metricsProvider);

        final String objectNameValue = createObjectName(taskName, queryMode, tableOrQuery);

        registerJmxBean(metricsProvider, objectNameValue);

        return metricsProvider;
    }

    private void registerJmxBean(final StartupMetric metricsProvider, final String objectNameValue) {
        try {
            final ObjectName objectName = new ObjectName(objectNameValue);
            final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean(metricsProvider, objectName);
        } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException
                 | MBeanRegistrationException e) {
            log.error(e.getMessage(), e);
        }
    }

    private String createObjectName(final String taskName, final CountQuerier.QueryMode queryMode,
                                    final String tableOrQuery) {
        final String topicName = config.getString(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);

        final String identifier = "io.aiven.connect.jdbc.initialImportCount";
        String objectNameValue = String.format("%s:task=\"%s\",topic=\"%s\"", identifier, taskName, topicName);
        if (queryMode == CountQuerier.QueryMode.TABLE) {
            objectNameValue += String.format(",table=\"%s\"", tableOrQuery);
        }

        log.info("CounterMetric Name: {}", objectNameValue);
        return objectNameValue;
    }
}
