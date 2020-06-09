/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.connect.jdbc.dialect;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialectProvider.FixedScoreProvider;
import io.aiven.connect.jdbc.sink.JdbcSinkConfig;
import io.aiven.connect.jdbc.sink.PreparedStatementBinder;
import io.aiven.connect.jdbc.sink.metadata.FieldsMetadata;
import io.aiven.connect.jdbc.sink.metadata.SchemaPair;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.source.ColumnMapping;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;
import io.aiven.connect.jdbc.source.JdbcSourceTaskConfig;
import io.aiven.connect.jdbc.source.TimestampIncrementingCriteria;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.DateTimeUtils;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.IdentifierRules;
import io.aiven.connect.jdbc.util.JdbcDriverInfo;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DatabaseDialect} implementation that provides functionality based upon JDBC and SQL.
 *
 * <p>This class is designed to be extended as required to customize the behavior for a specific
 * DBMS. For example, override the {@link #createColumnConverter(ColumnMapping)} method to customize
 * how a column value is converted to a field value for use in a {@link Struct}. To also change the
 * field's type or schema, also override the {@link #addFieldToSchema} method.
 */
public class GenericDatabaseDialect implements DatabaseDialect {

    protected static final int NUMERIC_TYPE_SCALE_LOW = -84;
    protected static final int NUMERIC_TYPE_SCALE_HIGH = 127;
    protected static final int NUMERIC_TYPE_SCALE_UNSET = -127;

    /**
     * The provider for {@link GenericDatabaseDialect}.
     */
    public static class Provider extends FixedScoreProvider {
        public Provider() {
            super(GenericDatabaseDialect.class.getSimpleName(),
                DatabaseDialectProvider.AVERAGE_MATCHING_SCORE
            );
        }

        @Override
        public DatabaseDialect create(final JdbcConfig config) {
            return new GenericDatabaseDialect(config);
        }
    }

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Whether to map {@code NUMERIC} JDBC types by precision.
     */
    protected final NumericMapping mapNumerics;
    protected String catalogPattern;
    protected final String schemaPattern;
    protected final Set<String> tableTypes;
    protected final String connectionUrl;
    protected final String connectionUsername;
    protected final Password connectionPassword;
    private final IdentifierRules defaultIdentifierRules;
    private final AtomicReference<IdentifierRules> identifierRules = new AtomicReference<>();
    private final Queue<Connection> connections = new ConcurrentLinkedQueue<>();
    private volatile JdbcDriverInfo jdbcDriverInfo;
    private final TimeZone timeZone;
    private final boolean quoteIdentifiers;

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public GenericDatabaseDialect(final JdbcConfig config) {
        this(config, IdentifierRules.DEFAULT);
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config                 the connector configuration; may not be null
     * @param defaultIdentifierRules the default rules for identifiers; may be null if the rules are
     *                               to be determined from the database metadata
     */
    protected GenericDatabaseDialect(
        final JdbcConfig config,
        final IdentifierRules defaultIdentifierRules
    ) {
        this.defaultIdentifierRules = defaultIdentifierRules;
        connectionUrl = config.getConnectionUrl();
        connectionUsername = config.getConnectionUser();
        connectionPassword = config.getConnectionPassword();
        if (config instanceof JdbcSinkConfig) {
            catalogPattern = JdbcSourceTaskConfig.CATALOG_PATTERN_DEFAULT;
            schemaPattern = JdbcSourceTaskConfig.SCHEMA_PATTERN_DEFAULT;
            tableTypes = new HashSet<>(Arrays.asList(JdbcSourceTaskConfig.TABLE_TYPE_DEFAULT));
        } else {
            catalogPattern = config.getString(JdbcSourceTaskConfig.CATALOG_PATTERN_CONFIG);
            schemaPattern = config.getString(JdbcSourceTaskConfig.SCHEMA_PATTERN_CONFIG);
            tableTypes = new HashSet<>(config.getList(JdbcSourceTaskConfig.TABLE_TYPE_CONFIG));
        }
        if (config instanceof JdbcSourceConnectorConfig) {
            mapNumerics = ((JdbcSourceConnectorConfig) config).numericMapping();
        } else {
            mapNumerics = NumericMapping.NONE;
        }

        timeZone = config.getDBTimeZone();
        quoteIdentifiers = config.isQuoteSqlIdentifiers();
    }

    @Override
    public String name() {
        return getClass().getSimpleName().replace("DatabaseDialect", "");
    }

    @Override
    public Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        if (this.connectionUsername != null) {
            properties.setProperty("user", this.connectionUsername);
        }
        if (this.connectionPassword != null) {
            properties.setProperty("password", this.connectionPassword.value());
        }
        properties = addConnectionProperties(properties);
        final Connection connection = DriverManager.getConnection(connectionUrl, properties);
        if (jdbcDriverInfo == null) {
            jdbcDriverInfo = createJdbcDriverInfo(connection);
        }
        connections.add(connection);
        return connection;
    }

    @Override
    public void close() {
        Connection conn;
        while ((conn = connections.poll()) != null) {
            try {
                conn.close();
            } catch (final Throwable e) {
                log.warn("Error while closing connection to {}", jdbcDriverInfo, e);
            }
        }
    }

    @Override
    public boolean isConnectionValid(
        final Connection connection,
        final int timeout
    ) throws SQLException {
        if (jdbcDriverInfo().jdbcMajorVersion() >= 4) {
            return connection.isValid(timeout);
        }
        // issue a test query ...
        final String query = checkConnectionQuery();
        if (query != null) {
            try (final Statement statement = connection.createStatement()) {
                if (statement.execute(query)) {
                    try (final ResultSet rs = statement.getResultSet()) {
                        // do nothing with the result set
                    }
                }
            }
        }
        return true;
    }

    /**
     * Return a query that can be used to check the validity of an existing database connection
     * when the JDBC driver does not support JDBC 4. By default this returns {@code SELECT 1},
     * but subclasses should override this when a different query should be used.
     *
     * @return the check connection query; may be null if the connection should not be queried
     */
    protected String checkConnectionQuery() {
        return "SELECT 1";
    }

    protected JdbcDriverInfo jdbcDriverInfo() {
        if (jdbcDriverInfo == null) {
            try (final Connection connection = getConnection()) {
                jdbcDriverInfo = createJdbcDriverInfo(connection);
            } catch (final SQLException e) {
                throw new ConnectException("Unable to get JDBC driver information", e);
            }
        }
        return jdbcDriverInfo;
    }

    protected JdbcDriverInfo createJdbcDriverInfo(final Connection connection) throws SQLException {
        final DatabaseMetaData metadata = connection.getMetaData();
        return new JdbcDriverInfo(
            metadata.getJDBCMajorVersion(),
            metadata.getJDBCMinorVersion(),
            metadata.getDriverName(),
            metadata.getDatabaseProductName(),
            metadata.getDatabaseProductVersion()
        );
    }

    /**
     * Add or modify any connection properties based upon the {@link #config configuration}.
     *
     * <p>By default this method does nothing and returns the {@link Properties} object supplied as a
     * parameter, but subclasses can override it to add/remove properties used to create new
     * connections.
     *
     * @param properties the properties that will be passed to the {@link DriverManager}'s {@link
     *                   DriverManager#getConnection(String, Properties) getConnection(...) method};
     *                   never null
     * @return the updated connection properties, or {@code properties} if they are not modified or
     *     should be returned; never null
     */
    protected Properties addConnectionProperties(final Properties properties) {
        return properties;
    }

    @Override
    public PreparedStatement createPreparedStatement(
        final Connection db,
        final String query
    ) throws SQLException {
        log.trace("Creating a PreparedStatement '{}'", query);
        final PreparedStatement stmt = db.prepareStatement(query);
        initializePreparedStatement(stmt);
        return stmt;
    }

    /**
     * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
     * the {@link #createPreparedStatement(Connection, String)} method after the statement is
     * created but before it is returned/used.
     *
     * <p>By default this method does nothing.
     *
     * @param stmt the prepared statement; never null
     * @throws SQLException the error that might result from initialization
     */
    protected void initializePreparedStatement(final PreparedStatement stmt) throws SQLException {
        // do nothing
    }

    @Override
    public TableId parseTableIdentifier(final String fqn) {
        final List<String> parts = identifierRules().parseQualifiedIdentifier(fqn);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Invalid fully qualified name: '" + fqn + "'");
        }
        if (parts.size() == 1) {
            return new TableId(null, null, parts.get(0));
        }
        if (parts.size() == 3) {
            return new TableId(parts.get(0), parts.get(1), parts.get(2));
        }
        assert parts.size() >= 2;
        if (useCatalog()) {
            return new TableId(parts.get(0), null, parts.get(1));
        }
        return new TableId(null, parts.get(0), parts.get(1));
    }

    /**
     * Return whether the database uses JDBC catalogs.
     *
     * @return true if catalogs are used, or false otherwise
     */
    protected boolean useCatalog() {
        return false;
    }

    @Override
    public List<TableId> tableIds(final Connection conn) throws SQLException {
        final DatabaseMetaData metadata = conn.getMetaData();
        final String[] tableTypes = tableTypes(metadata, this.tableTypes);

        try (final ResultSet rs = metadata.getTables(catalogPattern(), schemaPattern(), "%", tableTypes)) {
            final List<TableId> tableIds = new ArrayList<>();
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final TableId tableId = new TableId(catalogName, schemaName, tableName);
                if (includeTable(tableId)) {
                    tableIds.add(tableId);
                }
            }
            return tableIds;
        }
    }

    protected String catalogPattern() {
        return catalogPattern;
    }

    protected String schemaPattern() {
        return schemaPattern;
    }

    /**
     * Determine whether the table with the specific name is to be included in the tables.
     *
     * <p>This method can be overridden to exclude certain database tables.
     *
     * @param table the identifier of the table; may be null
     */
    protected boolean includeTable(final TableId table) {
        return true;
    }

    /**
     * Find the available table types that are returned by the JDBC driver that case insensitively
     * match the specified types.
     *
     * @param metadata the database metadata; may not be null but may be empty if no table types
     * @param types    the case-independent table types that are desired
     * @return the array of table types take directly from the list of available types returned by the
     *     JDBC driver; never null
     * @throws SQLException if there is an error with the database connection
     */
    protected String[] tableTypes(
        final DatabaseMetaData metadata,
        final Set<String> types
    ) throws SQLException {
        // Compute the uppercase form of the desired types ...
        final Set<String> uppercaseTypes = new HashSet<>();
        for (final String type : types) {
            if (type != null) {
                uppercaseTypes.add(type.toUpperCase(Locale.ROOT));
            }
        }
        // Now find out the available table types ...
        final Set<String> matchingTableTypes = new HashSet<>();
        try (final ResultSet rs = metadata.getTableTypes()) {
            while (rs.next()) {
                final String tableType = rs.getString(1);
                if (tableType != null && uppercaseTypes.contains(tableType.toUpperCase(Locale.ROOT))) {
                    matchingTableTypes.add(tableType);
                }
            }
        }
        return matchingTableTypes.toArray(new String[matchingTableTypes.size()]);
    }

    @Override
    public IdentifierRules identifierRules() {
        if (identifierRules.get() == null) {
            // Otherwise try to get the actual quote string and separator from the database, since
            // many databases allow them to be changed
            try (final Connection connection = getConnection()) {
                final DatabaseMetaData metaData = connection.getMetaData();
                String leadingQuoteStr = metaData.getIdentifierQuoteString();
                String trailingQuoteStr = leadingQuoteStr; // JDBC does not distinguish
                String separator = metaData.getCatalogSeparator();
                if (leadingQuoteStr == null || leadingQuoteStr.isEmpty()) {
                    leadingQuoteStr = defaultIdentifierRules.leadingQuoteString();
                    trailingQuoteStr = defaultIdentifierRules.trailingQuoteString();
                }
                if (separator == null || separator.isEmpty()) {
                    separator = defaultIdentifierRules.identifierDelimiter();
                }
                identifierRules.set(new IdentifierRules(separator, leadingQuoteStr, trailingQuoteStr));
            } catch (final SQLException e) {
                if (defaultIdentifierRules != null) {
                    identifierRules.set(defaultIdentifierRules);
                    log.warn("Unable to get identifier metadata; using default rules", e);
                } else {
                    throw new ConnectException("Unable to get identifier metadata", e);
                }
            }
        }
        return identifierRules.get();
    }

    @Override
    public ExpressionBuilder expressionBuilder() {
        return identifierRules().expressionBuilder(quoteIdentifiers);
    }

    /**
     * Return current time at the database
     *
     * @param conn database connection
     * @param cal  calendar
     * @return the current time at the database
     */
    @Override
    public Timestamp currentTimeOnDB(
        final Connection conn,
        final Calendar cal
    ) throws SQLException, ConnectException {
        final String query = currentTimestampDatabaseQuery();
        assert query != null;
        assert !query.isEmpty();
        try (final Statement stmt = conn.createStatement()) {
            log.debug("executing query " + query + " to get current time from database");
            try (final ResultSet rs = stmt.executeQuery(query)) {
                if (rs.next()) {
                    return rs.getTimestamp(1, cal);
                } else {
                    throw new ConnectException(
                        "Unable to get current time from DB using " + this + " and query '" + query + "'"
                    );
                }
            }
        } catch (final SQLException e) {
            log.error("Failed to get current time from DB using {} and query '{}'", this, query, e);
            throw e;
        }
    }

    /**
     * Get the query string to determine the current timestamp in the database.
     *
     * @return the query string; never null or empty
     */
    protected String currentTimestampDatabaseQuery() {
        return "SELECT CURRENT_TIMESTAMP";
    }

    @Override
    public boolean tableExists(
        final Connection connection,
        final TableId tableId
    ) throws SQLException {
        log.info("Checking {} dialect for existence of table {}", this, tableId);
        try (final ResultSet rs = connection.getMetaData().getTables(
            tableId.catalogName(),
            tableId.schemaName(),
            tableId.tableName(),
            new String[]{"TABLE"}
        )) {
            final boolean exists = rs.next();
            log.info("Using {} dialect table {} {}", this, tableId, exists ? "present" : "absent");
            return exists;
        }
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumns(
        final Connection connection,
        final String tablePattern,
        final String columnPattern
    ) throws SQLException {
        //if the table pattern is fqn, then just use the actual table name
        final TableId tableId = parseTableIdentifier(tablePattern);
        final String catalog = tableId.catalogName() != null ? tableId.catalogName() : catalogPattern;
        final String schema = tableId.schemaName() != null ? tableId.schemaName() : schemaPattern;
        return describeColumns(connection, catalog, schema, tableId.tableName(), columnPattern);
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumns(
        final Connection connection,
        final String catalogPattern,
        final String schemaPattern,
        final String tablePattern,
        final String columnPattern
    ) throws SQLException {
        log.debug(
            "Querying {} dialect column metadata for catalog:{} schema:{} table:{}",
            this,
            catalogPattern,
            schemaPattern,
            tablePattern
        );

        // Get the primary keys of the table(s) ...
        final Set<ColumnId> pkColumns = primaryKeyColumns(
            connection,
            catalogPattern,
            schemaPattern,
            tablePattern
        );
        final Map<ColumnId, ColumnDefinition> results = new HashMap<>();
        try (final ResultSet rs = connection.getMetaData().getColumns(
            catalogPattern,
            schemaPattern,
            tablePattern,
            columnPattern
        )) {
            final int rsColumnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final TableId tableId = new TableId(catalogName, schemaName, tableName);
                final String columnName = rs.getString(4);
                final ColumnId columnId = new ColumnId(tableId, columnName, null);
                final int jdbcType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int precision = rs.getInt(7);
                final int scale = rs.getInt(9);
                final String typeClassName = null;
                ColumnDefinition.Nullability nullability;
                final int nullableValue = rs.getInt(11);
                switch (nullableValue) {
                    case DatabaseMetaData.columnNoNulls:
                        nullability = ColumnDefinition.Nullability.NOT_NULL;
                        break;
                    case DatabaseMetaData.columnNullable:
                        nullability = ColumnDefinition.Nullability.NULL;
                        break;
                    case DatabaseMetaData.columnNullableUnknown:
                    default:
                        nullability = ColumnDefinition.Nullability.UNKNOWN;
                        break;
                }
                Boolean autoIncremented = null;
                if (rsColumnCount >= 23) {
                    // Not all drivers include all columns ...
                    final String isAutoIncremented = rs.getString(23);
                    if ("yes".equalsIgnoreCase(isAutoIncremented)) {
                        autoIncremented = Boolean.TRUE;
                    } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
                        autoIncremented = Boolean.FALSE;
                    }
                }
                final Boolean signed = null;
                final Boolean caseSensitive = null;
                final Boolean searchable = null;
                final Boolean currency = null;
                final Integer displaySize = null;
                final boolean isPrimaryKey = pkColumns.contains(columnId);
                if (isPrimaryKey) {
                    // Some DBMSes report pks as null
                    nullability = ColumnDefinition.Nullability.NOT_NULL;
                }
                final ColumnDefinition defn = columnDefinition(
                    rs,
                    columnId,
                    jdbcType,
                    typeName,
                    typeClassName,
                    nullability,
                    ColumnDefinition.Mutability.UNKNOWN,
                    precision,
                    scale,
                    signed,
                    displaySize,
                    autoIncremented,
                    caseSensitive,
                    searchable,
                    currency,
                    isPrimaryKey
                );
                results.put(columnId, defn);
            }
            return results;
        }
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumns(final ResultSetMetaData rsMetadata) throws
        SQLException {
        final Map<ColumnId, ColumnDefinition> result = new LinkedHashMap<>();
        for (int i = 1; i <= rsMetadata.getColumnCount(); ++i) {
            final ColumnDefinition defn = describeColumn(rsMetadata, i);
            result.put(defn.id(), defn);
        }
        return result;
    }

    /**
     * Create a definition for the specified column in the result set.
     *
     * @param rsMetadata the result set metadata; may not be null
     * @param column     the column number, starting at 1 for the first column
     * @return the column definition; never null
     * @throws SQLException if there is an error accessing the result set metadata
     */
    protected ColumnDefinition describeColumn(
        final ResultSetMetaData rsMetadata,
        final int column
    ) throws SQLException {
        final String catalog = rsMetadata.getCatalogName(column);
        final String schema = rsMetadata.getSchemaName(column);
        final String tableName = rsMetadata.getTableName(column);
        final TableId tableId = new TableId(catalog, schema, tableName);
        final String name = rsMetadata.getColumnName(column);
        final String alias = rsMetadata.getColumnLabel(column);
        final ColumnId id = new ColumnId(tableId, name, alias);
        final ColumnDefinition.Nullability nullability;
        switch (rsMetadata.isNullable(column)) {
            case ResultSetMetaData.columnNullable:
                nullability = ColumnDefinition.Nullability.NULL;
                break;
            case ResultSetMetaData.columnNoNulls:
                nullability = ColumnDefinition.Nullability.NOT_NULL;
                break;
            case ResultSetMetaData.columnNullableUnknown:
            default:
                nullability = ColumnDefinition.Nullability.UNKNOWN;
                break;
        }
        ColumnDefinition.Mutability mutability = ColumnDefinition.Mutability.MAYBE_WRITABLE;
        if (rsMetadata.isReadOnly(column)) {
            mutability = ColumnDefinition.Mutability.READ_ONLY;
        } else if (rsMetadata.isWritable(column)) {
            mutability = ColumnDefinition.Mutability.MAYBE_WRITABLE;
        } else if (rsMetadata.isDefinitelyWritable(column)) {
            mutability = ColumnDefinition.Mutability.WRITABLE;
        }
        return new ColumnDefinition(
            id,
            rsMetadata.getColumnType(column),
            rsMetadata.getColumnTypeName(column),
            rsMetadata.getColumnClassName(column),
            nullability,
            mutability,
            rsMetadata.getPrecision(column),
            rsMetadata.getScale(column),
            rsMetadata.isSigned(column),
            rsMetadata.getColumnDisplaySize(column),
            rsMetadata.isAutoIncrement(column),
            rsMetadata.isCaseSensitive(column),
            rsMetadata.isSearchable(column),
            rsMetadata.isCurrency(column),
            false
        );
    }

    protected Set<ColumnId> primaryKeyColumns(
        final Connection connection,
        final String catalogPattern,
        final String schemaPattern,
        final String tablePattern
    ) throws SQLException {

        // Get the primary keys of the table(s) ...
        final Set<ColumnId> pkColumns = new HashSet<>();
        try (final ResultSet rs = connection.getMetaData().getPrimaryKeys(
            catalogPattern, schemaPattern, tablePattern)) {
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final TableId tableId = new TableId(catalogName, schemaName, tableName);
                final String colName = rs.getString(4);
                final ColumnId columnId = new ColumnId(tableId, colName);
                pkColumns.add(columnId);
            }
        }
        return pkColumns;
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumnsByQuerying(
        final Connection db,
        final TableId tableId
    ) throws SQLException {
        final String queryStr = "SELECT * FROM {} LIMIT 1";
        final String quotedName = expressionBuilder().append(tableId).toString();
        try (final PreparedStatement stmt = db.prepareStatement(queryStr)) {
            stmt.setString(1, quotedName);
            try (final ResultSet rs = stmt.executeQuery()) {
                final ResultSetMetaData rsmd = rs.getMetaData();
                return describeColumns(rsmd);
            }
        }
    }

    @Override
    public TableDefinition describeTable(
        final Connection connection,
        final TableId tableId
    ) throws SQLException {
        final Map<ColumnId, ColumnDefinition> columnDefns = describeColumns(connection, tableId.catalogName(),
            tableId.schemaName(),
            tableId.tableName(), null
        );
        if (columnDefns.isEmpty()) {
            return null;
        }
        return new TableDefinition(tableId, columnDefns.values());
    }

    /**
     * Create a ColumnDefinition with supplied values and the result set from the {@link
     * DatabaseMetaData#getColumns(String, String, String, String)} call. By default that method does
     * not describe whether the column is signed, case sensitive, searchable, currency, or the
     * preferred display size.
     *
     * <p>Subclasses can override this method to extract additional non-standard characteristics from
     * the result set, and override the characteristics determined using the standard JDBC metadata
     * columns and supplied as parameters.
     *
     * @param resultSet        the result set
     * @param id               the column identifier
     * @param jdbcType         the JDBC type of the column
     * @param typeName         the name of the column's type
     * @param classNameForType the name of the class used as instances of the value when {@link
     *                         ResultSet#getObject(int)} is called
     * @param nullability      the nullability of the column
     * @param mutability       the mutability of the column
     * @param precision        the precision of the column for numeric values, or the length for
     *                         non-numeric values
     * @param scale            the scale of the column for numeric values; ignored for other values
     * @param signedNumbers    true if the column holds signed numeric values; null if not known
     * @param displaySize      the preferred display size for the column values; null if not known
     * @param autoIncremented  true if the column is auto-incremented; null if not known
     * @param caseSensitive    true if the column values are case-sensitive; null if not known
     * @param searchable       true if the column is searchable; null if no; null if not known known
     * @param currency         true if the column is a currency value
     * @param isPrimaryKey     true if the column is part of the primary key; null if not known known
     * @return the column definition; never null
     */
    protected ColumnDefinition columnDefinition(
        final ResultSet resultSet,
        final ColumnId id,
        final int jdbcType,
        final String typeName,
        final String classNameForType,
        final ColumnDefinition.Nullability nullability,
        final ColumnDefinition.Mutability mutability,
        final int precision,
        final int scale,
        final Boolean signedNumbers,
        final Integer displaySize,
        final Boolean autoIncremented,
        final Boolean caseSensitive,
        final Boolean searchable,
        final Boolean currency,
        final Boolean isPrimaryKey
    ) {
        return new ColumnDefinition(
            id,
            jdbcType,
            typeName,
            classNameForType,
            nullability,
            mutability,
            precision,
            scale,
            signedNumbers != null ? signedNumbers.booleanValue() : false,
            displaySize != null ? displaySize.intValue() : 0,
            autoIncremented != null ? autoIncremented.booleanValue() : false,
            caseSensitive != null ? caseSensitive.booleanValue() : false,
            searchable != null ? searchable.booleanValue() : false,
            currency != null ? currency.booleanValue() : false,
            isPrimaryKey != null ? isPrimaryKey.booleanValue() : false
        );
    }

    @Override
    public TimestampIncrementingCriteria criteriaFor(
        final ColumnId incrementingColumn,
        final List<ColumnId> timestampColumns
    ) {
        return new TimestampIncrementingCriteria(incrementingColumn, timestampColumns, timeZone);
    }

    /**
     * Determine the name of the field. By default this is the column alias or name.
     *
     * @param columnDefinition the column definition; never null
     * @return the field name; never null
     */
    protected String fieldNameFor(final ColumnDefinition columnDefinition) {
        return columnDefinition.id().aliasOrName();
    }

    @Override
    public String addFieldToSchema(
        final ColumnDefinition columnDefn,
        final SchemaBuilder builder
    ) {
        return addFieldToSchema(columnDefn, builder, fieldNameFor(columnDefn), columnDefn.type(),
            columnDefn.isOptional()
        );
    }

    /**
     * Use the supplied {@link SchemaBuilder} to add a field that corresponds to the column with the
     * specified definition. This is intended to be easily overridden by subclasses.
     *
     * @param columnDefn the definition of the column; may not be null
     * @param builder    the schema builder; may not be null
     * @param fieldName  the name of the field and {@link #fieldNameFor(ColumnDefinition) computed}
     *                   from the column definition; may not be null
     * @param sqlType    the JDBC {@link java.sql.Types type} as obtained from the column definition
     * @param optional   true if the field is to be optional as obtained from the column definition
     * @return the name of the field, or null if no field was added
     */
    protected String addFieldToSchema(
        final ColumnDefinition columnDefn,
        final SchemaBuilder builder,
        final String fieldName,
        final int sqlType,
        final boolean optional
    ) {
        final int precision = columnDefn.precision();
        int scale = columnDefn.scale();
        switch (sqlType) {
            case Types.NULL: {
                log.debug("JDBC type 'NULL' not currently supported for column '{}'", fieldName);
                return null;
            }

            case Types.BOOLEAN: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA);
                break;
            }

            // ints <= 8 bits
            case Types.BIT: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA);
                break;
            }

            case Types.TINYINT: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA);
                } else {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
                }
                break;
            }

            // 16 bit ints
            case Types.SMALLINT: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
                } else {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
                }
                break;
            }

            // 32 bit ints
            case Types.INTEGER: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
                } else {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
                }
                break;
            }

            // 64 bit ints
            case Types.BIGINT: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
                break;
            }

            // REAL is a single precision floating point value, i.e. a Java float
            case Types.REAL: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA);
                break;
            }

            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case Types.FLOAT:
            case Types.DOUBLE: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA);
                break;
            }

            case Types.NUMERIC:
                if (mapNumerics == NumericMapping.PRECISION_ONLY) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale == 0 && precision < 19) { // integer
                        final Schema schema;
                        if (precision > 9) {
                            schema = optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
                        } else if (precision > 4) {
                            schema = optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
                        } else if (precision > 2) {
                            schema = optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
                        } else {
                            schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
                        }
                        builder.field(fieldName, schema);
                        break;
                    }
                } else if (mapNumerics == NumericMapping.BEST_FIT) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (precision < 19) { // fits in primitive data types.
                        if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                            final Schema schema;
                            if (precision > 9) {
                                schema = optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
                            } else if (precision > 4) {
                                schema = optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
                            } else if (precision > 2) {
                                schema = optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
                            } else {
                                schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
                            }
                            builder.field(fieldName, schema);
                            break;
                        } else if (scale > 0) { // floating point - use double in all cases
                            final Schema schema = optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
                            builder.field(fieldName, schema);
                            break;
                        }
                    }
                }
                // fallthrough

            case Types.DECIMAL: {
                log.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, scale);
                scale = decimalScale(columnDefn);
                final SchemaBuilder fieldBuilder = Decimal.builder(scale);
                if (optional) {
                    fieldBuilder.optional();
                }
                builder.field(fieldName, fieldBuilder.build());
                break;
            }

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.DATALINK:
            case Types.SQLXML: {
                // Some of these types will have fixed size, but we drop this from the schema conversion
                // since only fixed byte arrays can have a fixed size
                builder.field(fieldName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
                break;
            }

            // Binary == fixed bytes
            // BLOB, VARBINARY, LONGVARBINARY == bytes
            case Types.BINARY:
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA);
                break;
            }

            // Date is day + moth + year
            case Types.DATE: {
                final SchemaBuilder dateSchemaBuilder = Date.builder();
                if (optional) {
                    dateSchemaBuilder.optional();
                }
                builder.field(fieldName, dateSchemaBuilder.build());
                break;
            }

            // Time is a time of day -- hour, minute, seconds, nanoseconds
            case Types.TIME: {
                final SchemaBuilder timeSchemaBuilder = Time.builder();
                if (optional) {
                    timeSchemaBuilder.optional();
                }
                builder.field(fieldName, timeSchemaBuilder.build());
                break;
            }

            // Timestamp is a date + time
            case Types.TIMESTAMP: {
                final SchemaBuilder tsSchemaBuilder = org.apache.kafka.connect.data.Timestamp.builder();
                if (optional) {
                    tsSchemaBuilder.optional();
                }
                builder.field(fieldName, tsSchemaBuilder.build());
                break;
            }

            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default: {
                log.warn("JDBC type {} ({}) not currently supported", sqlType, columnDefn.typeName());
                return null;
            }
        }
        return fieldName;
    }

    @Override
    public void applyDdlStatements(
        final Connection connection,
        final List<String> statements
    ) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            for (final String ddlStatement : statements) {
                statement.executeUpdate(ddlStatement);
            }
        }
    }

    @Override
    public ColumnConverter createColumnConverter(
        final ColumnMapping mapping
    ) {
        return columnConverterFor(mapping, mapping.columnDefn(), mapping.columnNumber(),
            jdbcDriverInfo().jdbcVersionAtLeast(4, 0)
        );
    }

    @SuppressWarnings("deprecation")
    protected ColumnConverter columnConverterFor(
        final ColumnMapping mapping,
        final ColumnDefinition defn,
        final int col,
        final boolean isJdbc4
    ) {
        switch (mapping.columnDefn().type()) {

            case Types.BOOLEAN: {
                return rs -> rs.getBoolean(col);
            }

            case Types.BIT: {
                /**
                 * BIT should be either 0 or 1.
                 * TODO: Postgres handles this differently, returning a string "t" or "f". See the
                 * elasticsearch-jdbc plugin for an example of how this is handled
                 */
                return rs -> rs.getByte(col);
            }

            // 8 bits int
            case Types.TINYINT: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getByte(col);
                } else {
                    return rs -> rs.getShort(col);
                }
            }

            // 16 bits int
            case Types.SMALLINT: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getShort(col);
                } else {
                    return rs -> rs.getInt(col);
                }
            }

            // 32 bits int
            case Types.INTEGER: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getInt(col);
                } else {
                    return rs -> rs.getLong(col);
                }
            }

            // 64 bits int
            case Types.BIGINT: {
                return rs -> rs.getLong(col);
            }

            // REAL is a single precision floating point value, i.e. a Java float
            case Types.REAL: {
                return rs -> rs.getFloat(col);
            }

            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case Types.FLOAT:
            case Types.DOUBLE: {
                return rs -> rs.getDouble(col);
            }

            case Types.NUMERIC:
                if (mapNumerics == NumericMapping.PRECISION_ONLY) {
                    final int precision = defn.precision();
                    final int scale = defn.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale == 0 && precision < 19) { // integer
                        if (precision > 9) {
                            return rs -> rs.getLong(col);
                        } else if (precision > 4) {
                            return rs -> rs.getInt(col);
                        } else if (precision > 2) {
                            return rs -> rs.getShort(col);
                        } else {
                            return rs -> rs.getByte(col);
                        }
                    }
                } else if (mapNumerics == NumericMapping.BEST_FIT) {
                    final int precision = defn.precision();
                    final int scale = defn.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (precision < 19) { // fits in primitive data types.
                        if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                            if (precision > 9) {
                                return rs -> rs.getLong(col);
                            } else if (precision > 4) {
                                return rs -> rs.getInt(col);
                            } else if (precision > 2) {
                                return rs -> rs.getShort(col);
                            } else {
                                return rs -> rs.getByte(col);
                            }
                        } else if (scale > 0) { // floating point - use double in all cases
                            return rs -> rs.getDouble(col);
                        }
                    }
                }
                // fallthrough

            case Types.DECIMAL: {
                final int precision = defn.precision();
                log.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, defn.scale());
                final int scale = decimalScale(defn);
                return rs -> rs.getBigDecimal(col, scale);
            }

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR: {
                return rs -> rs.getString(col);
            }

            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
                return rs -> rs.getNString(col);
            }

            // Binary == fixed, VARBINARY and LONGVARBINARY == bytes
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                return rs -> rs.getBytes(col);
            }

            // Date is day + month + year
            case Types.DATE: {
                return rs -> rs.getDate(col, DateTimeUtils.getTimeZoneCalendar(timeZone));
            }

            // Time is a time of day -- hour, minute, seconds, nanoseconds
            case Types.TIME: {
                return rs -> rs.getTime(col, DateTimeUtils.getTimeZoneCalendar(timeZone));
            }

            // Timestamp is a date + time
            case Types.TIMESTAMP: {
                return rs -> rs.getTimestamp(col, DateTimeUtils.getTimeZoneCalendar(timeZone));
            }

            // Datalink is basically a URL -> string
            case Types.DATALINK: {
                return rs -> {
                    final URL url = rs.getURL(col);
                    return url != null ? url.toString() : null;
                };
            }

            // BLOB == fixed
            case Types.BLOB: {
                return rs -> {
                    final Blob blob = rs.getBlob(col);
                    if (blob == null) {
                        return null;
                    } else {
                        try {
                            if (blob.length() > Integer.MAX_VALUE) {
                                throw new IOException("Can't process BLOBs longer than " + Integer.MAX_VALUE);
                            }
                            return blob.getBytes(1, (int) blob.length());
                        } finally {
                            if (isJdbc4) {
                                free(blob);
                            }
                        }
                    }
                };
            }
            case Types.CLOB:
                return rs -> {
                    final Clob clob = rs.getClob(col);
                    if (clob == null) {
                        return null;
                    } else {
                        try {
                            if (clob.length() > Integer.MAX_VALUE) {
                                throw new IOException("Can't process CLOBs longer than " + Integer.MAX_VALUE);
                            }
                            return clob.getSubString(1, (int) clob.length());
                        } finally {
                            if (isJdbc4) {
                                free(clob);
                            }
                        }
                    }
                };
            case Types.NCLOB: {
                return rs -> {
                    final Clob clob = rs.getNClob(col);
                    if (clob == null) {
                        return null;
                    } else {
                        try {
                            if (clob.length() > Integer.MAX_VALUE) {
                                throw new IOException("Can't process NCLOBs longer than " + Integer.MAX_VALUE);
                            }
                            return clob.getSubString(1, (int) clob.length());
                        } finally {
                            if (isJdbc4) {
                                free(clob);
                            }
                        }
                    }
                };
            }

            // XML -> string
            case Types.SQLXML: {
                return rs -> {
                    final SQLXML xml = rs.getSQLXML(col);
                    return xml != null ? xml.getString() : null;
                };
            }

            case Types.NULL:
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default: {
                // These are not currently supported, but we don't want to log something for every single
                // record we translate. There will already be errors logged for the schema translation
                break;
            }
        }
        return null;
    }

    protected int decimalScale(final ColumnDefinition defn) {
        return defn.scale() == NUMERIC_TYPE_SCALE_UNSET ? NUMERIC_TYPE_SCALE_HIGH : defn.scale();
    }

    /**
     * Called when the object has been fully read and {@link Blob#free()} should be called.
     *
     * @param blob the Blob; never null
     * @throws SQLException if there is a problem calling free()
     */
    protected void free(final Blob blob) throws SQLException {
        blob.free();
    }

    /**
     * Called when the object has been fully read and {@link Clob#free()} should be called.
     *
     * @param clob the Clob; never null
     * @throws SQLException if there is a problem calling free()
     */
    protected void free(final Clob clob) throws SQLException {
        clob.free();
    }

    @Override
    public String buildInsertStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        final ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String buildFirstMultiInsertStatement(
            final TableId table,
            final Collection<ColumnId> keyColumns,
            final Collection<ColumnId> nonKeyColumns
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String buildMultiInsertStatement(
            final TableId table,
            final Collection<ColumnId> keyColumns,
            final Collection<ColumnId> nonKeyColumns
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String buildUpdateStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        final ExpressionBuilder builder = expressionBuilder();
        builder.append("UPDATE ");
        builder.append(table);
        builder.append(" SET ");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
            .of(nonKeyColumns);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                .delimitedBy(" AND ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(keyColumns);
        }
        return builder.toString();
    }

    @Override
    public String buildUpsertQueryStatement(
        final TableId table,
        final Collection<ColumnId> keyColumns,
        final Collection<ColumnId> nonKeyColumns
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatementBinder statementBinder(
        final PreparedStatement statement,
        final JdbcSinkConfig.PrimaryKeyMode pkMode,
        final SchemaPair schemaPair,
        final FieldsMetadata fieldsMetadata,
        final JdbcSinkConfig.InsertMode insertMode
    ) {
        return new PreparedStatementBinder(
            this,
            statement,
            pkMode,
            schemaPair,
            fieldsMetadata,
            insertMode
        );
    }

    @Override
    public void bindField(
        final PreparedStatement statement,
        final int index,
        final Schema schema,
        final Object value
    ) throws SQLException {
        if (value == null) {
            statement.setObject(index, null);
        } else {
            boolean bound = maybeBindLogical(statement, index, schema, value);
            if (!bound) {
                bound = maybeBindPrimitive(statement, index, schema, value);
            }
            if (!bound) {
                throw new ConnectException("Unsupported source data type: " + schema.type());
            }
        }
    }

    protected boolean maybeBindPrimitive(
        final PreparedStatement statement,
        final int index,
        final Schema schema,
        final Object value
    ) throws SQLException {
        switch (schema.type()) {
            case INT8:
                statement.setByte(index, (Byte) value);
                break;
            case INT16:
                statement.setShort(index, (Short) value);
                break;
            case INT32:
                statement.setInt(index, (Integer) value);
                break;
            case INT64:
                statement.setLong(index, (Long) value);
                break;
            case FLOAT32:
                statement.setFloat(index, (Float) value);
                break;
            case FLOAT64:
                statement.setDouble(index, (Double) value);
                break;
            case BOOLEAN:
                statement.setBoolean(index, (Boolean) value);
                break;
            case STRING:
                statement.setString(index, (String) value);
                break;
            case BYTES:
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                statement.setBytes(index, bytes);
                break;
            default:
                return false;
        }
        return true;
    }

    protected boolean maybeBindLogical(
        final PreparedStatement statement,
        final int index,
        final Schema schema,
        final Object value
    ) throws SQLException {
        if (schema.name() != null) {
            switch (schema.name()) {
                case Date.LOGICAL_NAME:
                    statement.setDate(
                        index,
                        new java.sql.Date(((java.util.Date) value).getTime()),
                        DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                case Decimal.LOGICAL_NAME:
                    statement.setBigDecimal(index, (BigDecimal) value);
                    return true;
                case Time.LOGICAL_NAME:
                    statement.setTime(
                        index,
                        new java.sql.Time(((java.util.Date) value).getTime()),
                        DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    statement.setTimestamp(
                        index,
                        new java.sql.Timestamp(((java.util.Date) value).getTime()),
                        DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    @Override
    public String buildCreateTableStatement(
        final TableId table,
        final Collection<SinkRecordField> fields
    ) {
        final ExpressionBuilder builder = expressionBuilder();

        final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
        builder.append("CREATE TABLE ");
        builder.append(table);
        builder.append(" (");
        writeColumnsSpec(builder, fields);
        if (!pkFieldNames.isEmpty()) {
            builder.append(",");
            builder.append(System.lineSeparator());
            builder.append("PRIMARY KEY(");
            builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.identifiers())
                .of(pkFieldNames);
            builder.append(")");
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String buildDropTableStatement(
        final TableId table,
        final DropOptions options
    ) {
        final ExpressionBuilder builder = expressionBuilder();

        builder.append("DROP TABLE ");
        builder.append(table);
        if (options.ifExists()) {
            builder.append(" IF EXISTS");
        }
        if (options.cascade()) {
            builder.append(" CASCADE");
        }
        return builder.toString();
    }

    @Override
    public List<String> buildAlterTable(
        final TableId table,
        final Collection<SinkRecordField> fields
    ) {
        final boolean newlines = fields.size() > 1;

        final ExpressionBuilder.Transform<SinkRecordField> transform = (builder, field) -> {
            if (newlines) {
                builder.appendNewLine();
            }
            builder.append("ADD ");
            writeColumnSpec(builder, field);
        };

        final ExpressionBuilder builder = expressionBuilder();
        builder.append("ALTER TABLE ");
        builder.append(table);
        builder.append(" ");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(transform)
            .of(fields);
        return Collections.singletonList(builder.toString());
    }

    protected List<String> extractPrimaryKeyFieldNames(final Collection<SinkRecordField> fields) {
        final List<String> pks = new ArrayList<>();
        for (final SinkRecordField f : fields) {
            if (f.isPrimaryKey()) {
                pks.add(f.name());
            }
        }
        return pks;
    }

    protected void writeColumnsSpec(
        final ExpressionBuilder builder,
        final Collection<SinkRecordField> fields
    ) {
        final ExpressionBuilder.Transform<SinkRecordField> transform = (b, field) -> {
            b.append(System.lineSeparator());
            writeColumnSpec(b, field);
        };
        builder.appendList().delimitedBy(",").transformedBy(transform).of(fields);
    }

    protected void writeColumnSpec(
        final ExpressionBuilder builder,
        final SinkRecordField f
    ) {
        builder.appendIdentifier(f.name());
        builder.append(" ");
        final String sqlType = getSqlType(f);
        builder.append(sqlType);
        if (f.defaultValue() != null) {
            builder.append(" DEFAULT ");
            formatColumnValue(
                builder,
                f.schemaName(),
                f.schemaParameters(),
                f.schemaType(),
                f.defaultValue()
            );
        } else if (isColumnOptional(f)) {
            builder.append(" NULL");
        } else {
            builder.append(" NOT NULL");
        }
    }

    protected boolean isColumnOptional(final SinkRecordField field) {
        return field.isOptional();
    }

    protected void formatColumnValue(
        final ExpressionBuilder builder,
        final String schemaName,
        final Map<String, String> schemaParameters,
        final Schema.Type type,
        final Object value
    ) {
        if (schemaName != null) {
            switch (schemaName) {
                case Decimal.LOGICAL_NAME:
                    builder.append(value);
                    return;
                case Date.LOGICAL_NAME:
                    builder.appendStringQuoted(DateTimeUtils.formatDate((java.util.Date) value, timeZone));
                    return;
                case Time.LOGICAL_NAME:
                    builder.appendStringQuoted(DateTimeUtils.formatTime((java.util.Date) value, timeZone));
                    return;
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    builder.appendStringQuoted(
                        DateTimeUtils.formatTimestamp((java.util.Date) value, timeZone)
                    );
                    return;
                default:
                    // fall through to regular types
                    break;
            }
        }
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                // no escaping required
                builder.append(value);
                break;
            case BOOLEAN:
                // 1 & 0 for boolean is more portable rather than TRUE/FALSE
                builder.append((Boolean) value ? '1' : '0');
                break;
            case STRING:
                builder.appendStringQuoted(value);
                break;
            case BYTES:
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                builder.appendBinaryLiteral(bytes);
                break;
            default:
                throw new ConnectException("Unsupported type for column value: " + type);
        }
    }

    protected String getSqlType(final SinkRecordField f) {
        throw new ConnectException(String.format(
            "%s (%s) type doesn't have a mapping to the SQL database column type", f.schemaName(),
            f.schemaType()
        ));
    }

    /**
     * Return the sanitized form of the supplied JDBC URL, which masks any secrets or credentials.
     *
     * @param url the JDBC URL; may not be null
     * @return the sanitized URL; never null
     */
    protected String sanitizedUrl(final String url) {
        // Only replace standard URL-type properties ...
        return url.replaceAll("(?i)([?&]password=)[^&]*", "$1****");
    }

    @Override
    public String identifier() {
        return name() + " database " + sanitizedUrl(connectionUrl);
    }

    @Override
    public String toString() {
        return name();
    }
}
