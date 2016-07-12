package io.confluent.connect.jdbc.sink.writer;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import io.confluent.connect.jdbc.sink.ConnectionProvider;
import io.confluent.connect.jdbc.sink.Database;
import io.confluent.connect.jdbc.sink.binders.PreparedStatementBinder;
import io.confluent.connect.jdbc.sink.common.DatabaseMetadata;
import io.confluent.connect.jdbc.sink.common.DatabaseMetadataProvider;
import io.confluent.connect.jdbc.sink.common.ParameterValidator;
import io.confluent.connect.jdbc.sink.config.FieldsMappings;
import io.confluent.connect.jdbc.sink.config.JdbcSinkSettings;
import io.confluent.connect.jdbc.sink.dialect.DbDialect;

/**
 * Responsible for taking a sequence of SinkRecord and writing them to the database
 */
public final class JdbcDbWriter {
  private static final Logger logger = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final PreparedStatementContextIterable statementBuilder;
  private final Database database;
  private Connection connection = null;

  private final ConnectionProvider connectionProvider;

  /**
   * @param connectionProvider - The database connection provider
   * @param statementBuilder - Returns a sequence of PreparedStatement to process
   * @param database - Contains the database metadata (tables and their columns)
   */
  public JdbcDbWriter(final ConnectionProvider connectionProvider,
                      final PreparedStatementContextIterable statementBuilder,
                      final Database database) {
    ParameterValidator.notNull(connectionProvider, "connectionProvider");
    ParameterValidator.notNull(statementBuilder, "statementBuilder");
    ParameterValidator.notNull(database, "databaseChangesExecutor");

    this.connectionProvider = connectionProvider;
    this.statementBuilder = statementBuilder;
    this.database = database;
  }

  /**
   * Writes the given records to the database
   *
   * @param records - The sequence of records to insert
   */
  public void write(final Collection<SinkRecord> records) throws SQLException {
    if (records.isEmpty()) {
      logger.warn("Received empty sequence of SinkRecord");
    } else {
      final Iterator<PreparedStatementContext> iterator = statementBuilder.iterator(records);
      //initialize a new connection instance if we haven't done it before
      if (connection == null) {
        connection = connectionProvider.getConnection();
      } else if (!connection.isValid(3000)) {
        //check the connection is still valid
          logger.warn("The database connection is not valid. Reconnecting");
        closeConnectionQuietly();
        connection = connectionProvider.getConnection();
      }
      //begin transaction
      connection.setAutoCommit(false);

      int totalRecords = 0;

      //::hasNext can say it has items but ::next can actually throw NoSuchElementException
      //see the iterator where because of filtering we might not actually have data to process
      try {
        while (iterator.hasNext()) {

          final PreparedStatementContext statementContext = iterator.next();
          final PreparedStatementData statementData = statementContext.getPreparedStatementData();

          //handle possible database changes (new tables, new columns)
          database.update(statementContext.getTablesToColumnsMap(), connection);

          final String sql = statementData.getSql();
          logger.debug("Executing SQL: {}", sql);
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (Iterable<PreparedStatementBinder> entryBinders : statementData.getBinders()) {
              PreparedStatementBindData.apply(statement, entryBinders);
              statement.addBatch();
              totalRecords++;
            }
            statement.executeBatch();
          }
        }
      } catch (NoSuchElementException ex) {
        // FIXME WHT?
        //yes we can end up here; but it is not an issue
        logger.warn(ex.getMessage());
      }
      //commit the transaction
      connection.commit();

      logger.info("Wrote " + totalRecords + " to the database.");
      if (totalRecords == 0) {
        logger.warn("No records have been written. Given the configuration no data has been used from the SinkRecords.");
      }
    }
  }

  private void closeConnectionQuietly() {
    try {
      connection.close();
    } catch (SQLException sqle) {
      logger.warn("Ignoring error closing connection", sqle);
    }
  }

  public void close() {
    closeConnectionQuietly();
  }

  /**
   * Creates an instance of JdbcDbWriter from the Jdbc sink settings.
   *
   * @param settings - Holds the sink settings
   * @return Returns a new instsance of JdbcDbWriter
   */
  public static JdbcDbWriter from(final JdbcSinkSettings settings, final DatabaseMetadataProvider databaseMetadataProvider) throws SQLException {
    final ConnectionProvider connectionProvider = new ConnectionProvider(settings.getConnection(), settings.getUser(), settings.getPassword());

    final DatabaseMetadata databaseMetadata = databaseMetadataProvider.get(connectionProvider);

    final PreparedStatementContextIterable statementBuilder = PreparedStatementBuilderHelper.from(settings, databaseMetadata);

    final Set<String> tablesAllowingAutoCreate = new HashSet<>();
    final Set<String> tablesAllowingSchemaEvolution = new HashSet<>();

    for (FieldsMappings fm : settings.getMappings()) {
      if (fm.autoCreateTable()) {
        logger.info("Allowing auto-create for table {}", fm.getTableName());
        tablesAllowingAutoCreate.add(fm.getTableName());
      }
      if (fm.evolveTableSchema()) {
        logger.info("Allowing schema evolution for table {}", fm.getTableName());
        tablesAllowingSchemaEvolution.add(fm.getTableName());
      }
    }

    final DbDialect dbDialect = DbDialect.fromConnectionString(settings.getConnection());

    final Database database = new Database(
        tablesAllowingAutoCreate,
        tablesAllowingSchemaEvolution,
        databaseMetadata,
        dbDialect
    );

    return new JdbcDbWriter(connectionProvider, statementBuilder, database);
  }


  /**
   * Get the prepared statement builder
   *
   * @return a BatchedPreparedStatementBuilder
   */
  public PreparedStatementContextIterable getStatementBuilder() {
    return statementBuilder;
  }

}