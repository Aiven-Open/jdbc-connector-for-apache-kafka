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

package io.aiven.connect.jdbc.config;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import io.aiven.connect.jdbc.util.TimeZoneValidator;

public class JdbcConfig extends AbstractConfig {
    private static final String DATABASE_GROUP = "Database";

    public static final String CONNECTION_URL_CONFIG = "connection.url";
    private static final String CONNECTION_URL_DOC = "JDBC connection URL.";
    private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

    public static final String CONNECTION_USER_CONFIG = "connection.user";
    private static final String CONNECTION_USER_DOC = "JDBC connection user.";
    private static final String CONNECTION_USER_DISPLAY = "JDBC User";

    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

    public static final String DB_TIMEZONE_CONFIG = "db.timezone";
    private static final String DB_TIMEZONE_DEFAULT = "UTC";
    private static final String DB_TIMEZONE_CONFIG_DOC =
        "Name of the JDBC timezone that should be used in the connector when "
            + "querying with time-based criteria. Defaults to UTC.";
    private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB time zone";

    public static final String DIALECT_NAME_CONFIG = "dialect.name";
    private static final String DIALECT_NAME_DISPLAY = "Database Dialect";
    private static final String DIALECT_NAME_DEFAULT = "";
    private static final String DIALECT_NAME_DOC =
        "The name of the database dialect that should be used for this connector. By default this "
            + "is empty, and the connector automatically determines the dialect based upon the "
            + "JDBC connection URL. Use this if you want to override that behavior and use a "
            + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
            + "can be used.";

    public static final String ORACLE_ENCRYPTION_CLIENT_CONFIG = "oracle.net.encryption_client";
    private static final String ORACLE_ENCRYPTION_CLIENT_DISPLAY = "Oracle encryption";
    private static final String ORACLE_ENCRYPTION_CLIENT_DOC =
        "This parameter defines the level of security that the client wants to negotiate with "
            + "the server. The permitted values are \"REJECTED\", \"ACCEPTED\", \"REQUESTED\" and "
            + "\"REQUIRED\"";

    public static final String ORACLE_CHECKSUM_CLIENT_CONFIG = "oracle.net.crypto_checksum_client";
    private static final String ORACLE_CHECKSUM_CLIENT_DISPLAY = "Oracle crypto checksum";
    private static final String ORACLE_CHECKSUM_CLIENT_DOC =
        "This parameter defines the level of security that the client wants to negotiate with "
            + "the server for data integrity. The permitted values are \"REJECTED\", \"ACCEPTED\", "
            + "\"REQUESTED\" and \"REQUIRED\"";

    public static final String SQL_QUOTE_IDENTIFIERS_CONFIG = "sql.quote.identifiers";
    private static final Boolean SQL_QUOTE_IDENTIFIERS_DEFAULT = true;
    private static final String SQL_QUOTE_IDENTIFIERS_DOC =
        "Whether to delimit (in most databases, quote with double quotes) identifiers "
            + "(e.g., table names and column names) in SQL statements.";
    private static final String SQL_QUOTE_IDENTIFIERS_DISPLAY = "Quote SQL Identifiers";

    public JdbcConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    public final String getDialectName() {
        return getString(DIALECT_NAME_CONFIG);
    }

    public final String getConnectionUrl() {
        return getString(CONNECTION_URL_CONFIG);
    }

    public final String getConnectionUser() {
        return getString(CONNECTION_USER_CONFIG);
    }

    public final String getOracleEncryptionClient() {
        return getString(JdbcConfig.ORACLE_ENCRYPTION_CLIENT_CONFIG);
    }

    public final String getOracleChecksumClient() {
        return getString(JdbcConfig.ORACLE_CHECKSUM_CLIENT_CONFIG);
    }

    public final TimeZone getDBTimeZone() {
        return TimeZone.getTimeZone(ZoneId.of(getString(DB_TIMEZONE_CONFIG)));
    }

    public final Password getConnectionPassword() {
        return getPassword(CONNECTION_PASSWORD_CONFIG);
    }

    public final boolean isQuoteSqlIdentifiers() {
        return getBoolean(SQL_QUOTE_IDENTIFIERS_CONFIG);
    }

    protected static void defineConnectionUrl(final ConfigDef configDef,
                                              final int orderInGroup,
                                              final Collection<String> extraDependents) {
        configDef.define(
            CONNECTION_URL_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY,
            new ArrayList<>(extraDependents)
        );
    }

    protected static void defineConnectionUser(final ConfigDef configDef, final int orderInGroup) {
        configDef.define(
            CONNECTION_USER_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_USER_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.MEDIUM,
            CONNECTION_USER_DISPLAY
        );
    }

    protected static void defineConnectionPassword(final ConfigDef configDef, final int orderInGroup) {
        configDef.define(
            CONNECTION_PASSWORD_CONFIG,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_PASSWORD_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.MEDIUM,
            CONNECTION_PASSWORD_DISPLAY
        );
    }

    protected static void defineDbTimezone(final ConfigDef configDef, final int orderInGroup) {
        configDef.define(
            DB_TIMEZONE_CONFIG,
            ConfigDef.Type.STRING,
            DB_TIMEZONE_DEFAULT,
            TimeZoneValidator.INSTANCE,
            ConfigDef.Importance.MEDIUM,
            DB_TIMEZONE_CONFIG_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.MEDIUM,
            DB_TIMEZONE_CONFIG_DISPLAY
        );
    }

    protected static void defineDialectName(final ConfigDef configDef, final int orderInGroup) {
        configDef.define(
            DIALECT_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DIALECT_NAME_DEFAULT,
            DatabaseDialectRecommender.INSTANCE,
            ConfigDef.Importance.LOW,
            DIALECT_NAME_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.MEDIUM,
            DIALECT_NAME_DISPLAY,
            DatabaseDialectRecommender.INSTANCE
        );
    }

    protected static void defineOracleEncryptionClient(final ConfigDef configDef, final int orderInGroup) {
        configDef.define(
            JdbcConfig.ORACLE_ENCRYPTION_CLIENT_CONFIG,
            ConfigDef.Type.STRING,
            null,
            null,
            ConfigDef.Importance.LOW,
            JdbcConfig.ORACLE_ENCRYPTION_CLIENT_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.LONG,
            JdbcConfig.ORACLE_ENCRYPTION_CLIENT_DISPLAY
        );
    }

    protected static void defineOracleChecksumClient(final ConfigDef configDef, final int orderInGroup) {
        configDef.define(
            JdbcConfig.ORACLE_CHECKSUM_CLIENT_CONFIG,
            ConfigDef.Type.STRING,
            null,
            null,
            ConfigDef.Importance.LOW,
            JdbcConfig.ORACLE_CHECKSUM_CLIENT_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.LONG,
            JdbcConfig.ORACLE_CHECKSUM_CLIENT_DISPLAY
        );
    }

    protected static void defineSqlQuoteIdentifiers(final ConfigDef configDef, final int orderInGroup) {
        configDef.define(
            JdbcConfig.SQL_QUOTE_IDENTIFIERS_CONFIG,
            ConfigDef.Type.BOOLEAN,
            JdbcConfig.SQL_QUOTE_IDENTIFIERS_DEFAULT,
            ConfigDef.Importance.LOW,
            JdbcConfig.SQL_QUOTE_IDENTIFIERS_DOC,
            DATABASE_GROUP,
            orderInGroup,
            ConfigDef.Width.SHORT,
            JdbcConfig.SQL_QUOTE_IDENTIFIERS_DISPLAY
        );
    }
}
