package io.confluent.connect.jdbc.sink.writer;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
  private final ErrorHandlingPolicy errorHandlingPolicy;
  private final Database database;
  private int retries;
  private Date lastError;
  private String lastErrorMessage;
  private final int maxRetries;
  private Connection connection = null;

  //provides connection pooling
  private final ConnectionProvider connectionProvider;

  /**
   * @param connectionProvider - The database connection provider
   * @param statementBuilder - Returns a sequence of PreparedStatement to process
   * @param errorHandlingPolicy - An instance of the error handling approach
   * @param database - Contains the database metadata (tables and their columns)
   * @param retries - Number of attempts to run when a SQLException occurs
   */
  public JdbcDbWriter(final ConnectionProvider connectionProvider,
                      final PreparedStatementContextIterable statementBuilder,
                      final ErrorHandlingPolicy errorHandlingPolicy,
                      final Database database,
                      final int retries) {
    ParameterValidator.notNull(connectionProvider, "connectionProvider");
    ParameterValidator.notNull(statementBuilder, "statementBuilder");
    ParameterValidator.notNull(database, "databaseChangesExecutor");

    this.connectionProvider = connectionProvider;
    this.statementBuilder = statementBuilder;
    this.errorHandlingPolicy = errorHandlingPolicy;
    this.database = database;
    this.retries = retries;
    this.maxRetries = retries;
  }

  /**
   * Writes the given records to the database
   *
   * @param records - The sequence of records to insert
   */
  public void write(final Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      logger.warn("Received empty sequence of SinkRecord");
    } else {

      final Iterator<PreparedStatementContext> iterator = statementBuilder.iterator(records);
      try {
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
          //yes we can end up here; but it is not an issue
          logger.warn(ex.getMessage());
        }
        //commit the transaction
        connection.commit();

        logger.info("Wrote " + totalRecords + " to the database.");
        if (maxRetries != retries) {
          retries = maxRetries;
          SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'");
          logger.info("Recovered from error '{}' ({})", lastErrorMessage, formatter.format(lastError));
        }
        if (totalRecords == 0) {
          logger.warn("No records have been written. Given the configuration no data has been used from the SinkRecords.");
        }
      } catch (SQLException sqlException) {
        final SinkRecord firstRecord = records.iterator().next();
        logger.error("Error occurred inserting data starting at topic:{} offset:{} partition:{}",
                     firstRecord.topic(),
                     firstRecord.kafkaOffset(),
                     firstRecord.kafkaPartition(),
                     sqlException);

        SQLException inner = sqlException.getNextException();
        while (inner != null) {
          logger.error(inner.getMessage(), inner);
          inner = inner.getNextException();
        }

        if (connection != null) {
          //rollback the transaction
          try {
            connection.rollback();
          } catch (Throwable t) {
            logger.error(t.getMessage());
          }
        }

        retries--;
        lastError = new Date();
        lastErrorMessage = sqlException.getMessage();
        errorHandlingPolicy.handle(records, sqlException, retries);
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
  public static JdbcDbWriter from(final JdbcSinkSettings settings,
                                  final DatabaseMetadataProvider databaseMetadataProvider)
      throws SQLException {

    final ConnectionProvider connectionProvider = new ConnectionProvider(settings.getConnection(),
                                                                         settings.getUser(),
                                                                         settings.getPassword(),
                                                                         settings.getRetries(),
                                                                         settings.getRetryDelay());

    final DatabaseMetadata databaseMetadata = databaseMetadataProvider.get(connectionProvider);

    final PreparedStatementContextIterable statementBuilder = PreparedStatementBuilderHelper.from(settings, databaseMetadata);

    final ErrorHandlingPolicy errorHandlingPolicy = ErrorHandlingPolicyHelper.from(settings.getErrorPolicy());
    logger.info("Created the error policy handler as {}", errorHandlingPolicy.getClass().getCanonicalName());

    final Set<String> tablesAllowingAutoCreate = new HashSet<>();
    final Set<String> tablesAllowingSchemaEvolution = new HashSet<>();

    validateSettings(settings, tablesAllowingAutoCreate, tablesAllowingSchemaEvolution);

    final DbDialect dbDialect = DbDialect.fromConnectionString(settings.getConnection());

    final Database database = new Database(
        tablesAllowingAutoCreate,
        tablesAllowingSchemaEvolution,
        databaseMetadata,
        dbDialect,
        settings.getRetries());

    return new JdbcDbWriter(connectionProvider,
                            statementBuilder,
                            errorHandlingPolicy,
                            database,
                            settings.getRetries());
  }


  private static void validateSettings(JdbcSinkSettings settings,
                                       Set<String> tablesAllowingAutoCreate,
                                       Set<String> tablesAllowingSchemaEvolution) {
    final List<FieldsMappings> mappingsList = settings.getMappings();
    for (FieldsMappings fm : mappingsList) {
      if (fm.autoCreateTable()) {
        logger.info("Allowing auto-create for table {}", fm.getTableName());
        tablesAllowingAutoCreate.add(fm.getTableName());
      }
      if (fm.evolveTableSchema()) {
        logger.info("Allowing schema evolution for table {}", fm.getTableName());
        tablesAllowingSchemaEvolution.add(fm.getTableName());
      }
    }
  }

  /**
   * Get the prepared statement builder
   *
   * @return a BatchedPreparedStatementBuilder
   */
  public PreparedStatementContextIterable getStatementBuilder() {
    return statementBuilder;
  }

  /**
   * Get the Error Handling Policy for the task
   *
   * @return A ErrorHandlingPolicy
   */
  public ErrorHandlingPolicy getErrorHandlingPolicy() {
    return errorHandlingPolicy;
  }
}