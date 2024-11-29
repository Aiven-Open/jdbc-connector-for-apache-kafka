package io.aiven.connect.jdbc.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple cache of {@link TableDefinition} keyed.
 */
public class TableDefinitions {

    private static final Logger log = LoggerFactory.getLogger(TableDefinitions.class);

    private final Map<TableId, TableDefinition> cache = new HashMap<>();
    private final DatabaseDialect dialect;

    /**
     * Create an instance that uses the specified database dialect.
     *
     * @param dialect the database dialect; may not be null
     */
    public TableDefinitions(final DatabaseDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Get the {@link TableDefinition} for the given table.
     *
     * @param connection the JDBC connection to use; may not be null
     * @param tableId    the table identifier; may not be null
     * @return the cached {@link TableDefinition}, or null if there is no such table
     * @throws SQLException if there is any problem using the connection
     */
    public TableDefinition get(
            final Connection connection,
            final TableId tableId
    ) throws SQLException {
        TableDefinition dbTable = cache.get(tableId);
        if (dbTable == null) {
            if (dialect.tableExists(connection, tableId)) {
                dbTable = dialect.describeTable(connection, tableId);
                if (dbTable != null) {
                    log.info("Setting metadata for table {} to {}", tableId, dbTable);
                    cache.put(tableId, dbTable);
                }
            }
        }
        return dbTable;
    }

    /**
     * Refresh the cached {@link TableDefinition} for the given table.
     *
     * @param connection the JDBC connection to use; may not be null
     * @param tableId    the table identifier; may not be null
     * @return the refreshed {@link TableDefinition}, or null if there is no such table
     * @throws SQLException if there is any problem using the connection
     */
    public TableDefinition refresh(
            final Connection connection,
            final TableId tableId
    ) throws SQLException {
        final TableDefinition dbTable = dialect.describeTable(connection, tableId);
        log.info("Refreshing metadata for table {} to {}", tableId, dbTable);
        cache.put(dbTable.id(), dbTable);
        return dbTable;
    }

    /**
     * Get the TableDefinition for a table, ensuring it is initialized in cache.
     *
     * @param connection the JDBC connection to use; may not be null
     * @param tableId    the table identifier; may not be null
     * @return the {@link TableDefinition} for the table
     * @throws SQLException if there is any problem using the connection
     */
    public TableDefinition tableDefinitionFor(
            final Connection connection,
            final TableId tableId
    ) throws SQLException {
        TableDefinition tableDefn = get(connection, tableId);
        if (tableDefn == null) {
            tableDefn = refresh(connection, tableId);
        }
        return tableDefn;
    }
}
