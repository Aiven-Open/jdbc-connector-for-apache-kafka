/**
 * Copyright 2015 Datamountaineer.
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


package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.datamountaineer.streamreactor.connect.config.PayloadFields;
import com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcDriverLoader;
import io.confluent.common.config.ConfigException;

import java.io.File;

/**
 * Holds the Jdbc Sink settings
 */
public final class JdbcSinkSettings {
    private final String connection;
    private final String tableName;
    private final PayloadFields fields;
    private final boolean batching;
    private final ErrorPolicyEnum errorPolicy;

    public JdbcSinkSettings(String connection, String tableName, PayloadFields fields, boolean batching, ErrorPolicyEnum errorPolicy) {
        this.connection = connection;
        this.tableName = tableName;
        this.fields = fields;
        this.batching = batching;
        this.errorPolicy = errorPolicy;
    }

    public String getConnection() {
        return connection;
    }

    public String getTableName() {
        return tableName;
    }

    public PayloadFields getFields() {
        return fields;
    }

    public boolean isBatching() {
        return batching;
    }

    public ErrorPolicyEnum getErrorPolicy() {
        return errorPolicy;
    }

    @Override
    public String toString() {
        return String.format("JdbcSinkSettings(\n" +
                        "connection=%s\n" +
                        "table name=%s\n" +
                        "fields=%s\n" +
                        "error policy=%s\n" +
                        ")"
                , connection, tableName, fields.toString(), errorPolicy.toString());
    }

    /**
     * Creates an instance of JdbcSinkSettings from a JdbcSinkConfig
     *
     * @param config : The map of all provided configurations
     * @return An instance of JdbcSinkSettings
     */
    public static JdbcSinkSettings from(final JdbcSinkConfig config) {

        final String driverClass = config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS);
        final File jarFile = new File(config.getString(JdbcSinkConfig.JAR_FILE));
        if (!jarFile.exists())
            throw new ConfigException(jarFile + " doesn't exist");

        JdbcDriverLoader.load(driverClass, jarFile);

        return new JdbcSinkSettings(
                config.getString(JdbcSinkConfig.DATABASE_CONNECTION),
                config.getString(JdbcSinkConfig.DATABASE_TABLE),
                PayloadFields.from(config.getString(JdbcSinkConfig.FIELDS)),
                config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING),
                ErrorPolicyEnum.valueOf(config.getString(JdbcSinkConfig.ERROR_POLICY)));
    }
}