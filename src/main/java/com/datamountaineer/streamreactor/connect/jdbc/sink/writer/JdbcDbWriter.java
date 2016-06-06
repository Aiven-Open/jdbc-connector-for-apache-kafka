/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.AutoCloseableHelper;
import com.datamountaineer.streamreactor.connect.jdbc.ConnectionProvider;
import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadataProvider;
import com.datamountaineer.streamreactor.connect.jdbc.common.ParameterValidator;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.DbDialect;
import com.datamountaineer.streamreactor.connect.jdbc.sink.Database;
import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;
import com.datamountaineer.streamreactor.connect.jdbc.sink.avro.AvroToDbConverter;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.google.common.collect.Iterators;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Responsible for taking a sequence of SinkRecord and writing them to the database
 */
public final class JdbcDbWriter implements DbWriter {
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
   * @param connectionProvider  - The database connection provider
   * @param statementBuilder    - Returns a sequence of PreparedStatement to process
   * @param errorHandlingPolicy - An instance of the error handling approach
   * @param database            - Contains the database metadata (tables and their columns)
   * @param retries             - Number of attempts to run when a SQLException occurs
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
  @Override
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
          AutoCloseableHelper.close(connection);
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

            PreparedStatement statement = null;
            try {
              final String sql = statementData.getSql();
              logger.debug(String.format("Executing SQL:\n%s", sql));
              statement = connection.prepareStatement(sql);
              for (Iterable<PreparedStatementBinder> entryBinders : statementData.getBinders()) {
                PreparedStatementBindData.apply(statement, entryBinders);
                statement.addBatch();
                totalRecords++;
              }
              statement.executeBatch();

            } finally {
              if (statement != null) {
                statement.close();
              }
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
          logger.info(String.format("Recovered from error % at %s", formatter.format(lastError), lastErrorMessage));
        }
        if (totalRecords == 0) {
          logger.warn("No records have been written. Given the configuration no data has been used from the SinkRecords.");
        }
      } catch (SQLException sqlException) {
        final SinkRecord firstRecord = Iterators.getNext(records.iterator(), null);
        assert firstRecord != null;
        logger.error(String.format("Following error has occurred inserting data starting at topic:%s offset:%d partition:%d",
                firstRecord.topic(),
                firstRecord.kafkaOffset(),
                firstRecord.kafkaPartition()));
        logger.error(sqlException.getMessage(), sqlException);

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

  @Override
  public void close() {
    AutoCloseableHelper.close(connection);
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
    logger.info(String.format("Created the error policy handler as %s", errorHandlingPolicy.getClass().getCanonicalName()));

    final Set<String> tablesAllowingAutoCreate = new HashSet<>();
    final Set<String> tablesAllowingSchemaEvolution = new HashSet<>();
    final Map<String, Collection<SinkRecordField>> createTablesMap = new HashMap<>();

    validateSettings(settings, tablesAllowingAutoCreate, tablesAllowingSchemaEvolution, createTablesMap);

    final DbDialect dbDialect = DbDialect.fromConnectionString(settings.getConnection());

    final Database database = new Database(
            tablesAllowingAutoCreate,
            tablesAllowingSchemaEvolution,
            databaseMetadata,
            dbDialect,
            settings.getRetries());

    //create an required tables
    if (!createTablesMap.isEmpty()) {
      database.createTables(createTablesMap, connectionProvider.getConnection());
    }

    return new JdbcDbWriter(connectionProvider,
            statementBuilder,
            errorHandlingPolicy,
            database,
            settings.getRetries());
  }


  private static void validateSettings(JdbcSinkSettings settings,
                                       Set<String> tablesAllowingAutoCreate,
                                       Set<String> tablesAllowingSchemaEvolution,
                                       Map<String, Collection<SinkRecordField>> createTablesMap) {

    final List<FieldsMappings> mappingsList = settings.getMappings();
    //for the mappings get the schema from the schema registry and add the default pk col.
    for (FieldsMappings fm : mappingsList) {
      if (fm.autoCreateTable()) {
        tablesAllowingAutoCreate.add(fm.getTableName());
        RestService registry = new RestService(settings.getSchemaRegistryUrl());
        List<String> all = new ArrayList<>();

        try {
          all = registry.getAllSubjects();
        } catch (RestClientException e) {
          logger.info(String.format(
                  "No schemas found in Registry! Waiting for first record to create table for topic %s", fm.getIncomingTopic()));
        } catch (IOException e) {
          logger.error(
                  String.format("Unable to connect to the Schema Registry at %s %s",
                          settings.getSchemaRegistryUrl(),
                          e.getMessage()),
                  e);
        }

        String lkTopic = fm.getIncomingTopic();
        //do we have our topic
        if (!all.contains(lkTopic)) {
          //try topic name + value
          if (all.contains(lkTopic + "-value")) {
            lkTopic = lkTopic + "-value";
          }
        }

        String latest = null;
        logger.info("Looking for schema " + lkTopic);
        try {
          latest = registry.getLatestVersion(lkTopic).getSchema();
          logger.info(String.format("Found the following schema in the Registry for topic %s%s%s ",
                  lkTopic,
                  System.lineSeparator(),
                  latest));
          AvroToDbConverter converter = new AvroToDbConverter();
          Collection<SinkRecordField> convertedFields = converter.convert(latest, fm.getMappings());

          logger.info("Field mappings");
          for (Map.Entry<String, FieldAlias> f : fm.getMappings().entrySet()) {
            logger.info(f.getKey() + " " + f.getValue());
          }

          //do we have a default pk columns
          FieldAlias pk = fm.getMappings().get(FieldsMappings.CONNECT_TOPIC_COLUMN);
          if (pk != null) {
            //add pk column if we have it to schema registry list of columns.
            logger.info("Adding default primary key (" + FieldsMappings.CONNECT_TOPIC_COLUMN + "," +
                    FieldsMappings.CONNECT_PARTITION_COLUMN + "," + FieldsMappings.CONNECT_OFFSET_COLUMN + ")");
            convertedFields.add(new SinkRecordField(Schema.Type.STRING, FieldsMappings.CONNECT_TOPIC_COLUMN, true));
            convertedFields.add(new SinkRecordField(Schema.Type.INT32, FieldsMappings.CONNECT_PARTITION_COLUMN, true));
            convertedFields.add(new SinkRecordField(Schema.Type.INT64, FieldsMappings.CONNECT_OFFSET_COLUMN, true));
            createTablesMap.put(fm.getTableName(), convertedFields);
          }
        } catch (RestClientException e) {
          logger.info(String.format(
                  "No schema found in Registry! Waiting for first record to create table for topic %s",
                  fm.getIncomingTopic()));
        } catch (IOException e) {
          logger.error(String.format("Unable to connect to the Schema Registry at %s %s",
                  settings.getSchemaRegistryUrl(),
                  e.getMessage()),
                  e);
        }
      }

      if (fm.evolveTableSchema()) {
        logger.info(String.format("Allowing schema evolution for table %s", fm.getTableName()));
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