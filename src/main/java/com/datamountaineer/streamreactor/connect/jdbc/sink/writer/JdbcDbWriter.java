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

import com.datamountaineer.streamreactor.connect.jdbc.sink.DatabaseChangesExecutor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.sink.DbWriter;
import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.DbDialect;
import com.google.common.collect.Iterators;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Responsible for taking a sequence of SinkRecord and writing them to the database
 */
public final class JdbcDbWriter implements DbWriter {
  private static final Logger logger = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final PreparedStatementBuilder statementBuilder;
  private final ErrorHandlingPolicy errorHandlingPolicy;
  private final DatabaseChangesExecutor databaseChangesExecutor;
  private int retries;

  //provides connection pooling
  private final HikariDataSource dataSource;

  /**
   * @param dbUri                   - The database connection string
   * @param user-                   - The database user to connect as
   * @param password                - The database user password
   * @param statementBuilder        - Returns a sequence of PreparedStatement to process
   * @param errorHandlingPolicy     - An instance of the error handling approach
   * @param databaseChangesExecutor -Contains the database metadata (tables and their columns)
   * @param retries                 - Number of attempts to run when a SQLException occurs
   */
  public JdbcDbWriter(final String dbUri,
                      final String user,
                      final String password,
                      final PreparedStatementBuilder statementBuilder,
                      final ErrorHandlingPolicy errorHandlingPolicy,
                      final DatabaseChangesExecutor databaseChangesExecutor,
                      final int retries) {
    ParameterValidator.notNullOrEmpty(dbUri, "dbUri");
    ParameterValidator.notNull(statementBuilder, "statementBuilder");
    ParameterValidator.notNull(databaseChangesExecutor, "databaseChangesExecutor");

    final HikariConfig config = new HikariConfig();
    config.setJdbcUrl(dbUri);

    if (user != null && user.trim().length() > 0) {
      config.setUsername(user);
    }

    if (password != null && password.trim().length() > 0) {
      config.setPassword(password);
    }
    this.dataSource = new HikariDataSource(config);
    this.statementBuilder = statementBuilder;
    this.errorHandlingPolicy = errorHandlingPolicy;
    this.databaseChangesExecutor = databaseChangesExecutor;
    this.retries = retries;
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

      Connection connection = null;
      Collection<PreparedStatement> statements = null;
      try {
        connection = dataSource.getConnection();
        final PreparedStatementContext statementContext = statementBuilder.build(records, connection);
        statements = statementContext.getPreparedStatements();
        if (!statements.isEmpty()) {

          //begin transaction
          connection.setAutoCommit(false);
          //handle possible database changes (new tables, new columns)
          databaseChangesExecutor.handleChanges(statementContext.getTablesToColumnsMap(), connection);

          for (final PreparedStatement statement : statements) {
            if (statementBuilder.isBatching()) {
              statement.executeBatch();
            } else {
              statement.execute();
            }
          }
          //commit the transaction
          connection.commit();
        }

      } catch (SQLException sqlException) {
        final SinkRecord firstRecord = Iterators.getNext(records.iterator(), null);
        assert firstRecord != null;
        logger.error(String.format("Following error has occurred inserting data starting at topic:%s offset:%d partition:%d",
                firstRecord.topic(),
                firstRecord.kafkaOffset(),
                firstRecord.kafkaPartition()));
        logger.error(sqlException.getMessage());

        if (connection != null) {
          //rollback the transaction
          try {
            connection.rollback();
          } catch (Throwable t) {
            logger.error(t.getMessage());
          }
        }

        //handle the exception
        retries--;
        errorHandlingPolicy.handle(records, sqlException, retries);
      } finally {
        if (statements != null) {
          for (final PreparedStatement statement : statements) {
            try {
              statement.close();
            } catch (Throwable t) {
              logger.error(t.getMessage());
            }
          }
        }

        if (connection != null) {
          try {
            connection.close();
          } catch (Throwable t) {
            logger.error(t.getMessage());
          }
        }
      }
    }
  }

  @Override
  public void close() {
    dataSource.close();
  }

  /**
   * Creates an instance of JdbcDbWriter from the Jdbc sink settings.
   *
   * @param settings         - Holds the sink settings
   * @param databaseMetadata
   * @return Returns a new instsance of JdbcDbWriter
   */
  public static JdbcDbWriter from(final JdbcSinkSettings settings,
                                  final DatabaseMetadata databaseMetadata) {

    final PreparedStatementBuilder statementBuilder = PreparedStatementBuilderHelper.from(settings, databaseMetadata);
    logger.info(String.format("Created PreparedStatementBuilder as %s", statementBuilder.getClass().getCanonicalName()));
    final ErrorHandlingPolicy errorHandlingPolicy = ErrorHandlingPolicyHelper.from(settings.getErrorPolicy());
    logger.info(String.format("Created the error policy handler as %s", errorHandlingPolicy.getClass().getCanonicalName()));

    final List<FieldsMappings> mappingsList = settings.getMappings();
    final Set<String> tablesAllowingAutoCreate = new HashSet<>();
    final Set<String> tablesAllowingSchemaEvolution = new HashSet<>();
    for (FieldsMappings fm : mappingsList) {
      if (fm.autoCreateTable()) {
        tablesAllowingAutoCreate.add(fm.getTableName());
      }
      if (fm.evolveTableSchema()) {
        tablesAllowingSchemaEvolution.add(fm.getTableName());
      }
    }

    final DbDialect dbDialect = DbDialect.fromConnectionString(settings.getConnection());

    final DatabaseChangesExecutor databaseChangesExecutor = new DatabaseChangesExecutor(tablesAllowingAutoCreate,
            tablesAllowingSchemaEvolution,
            databaseMetadata,
            dbDialect);

    return new JdbcDbWriter(settings.getConnection(),
            settings.getUser(),
            settings.getPassword(),
            statementBuilder,
            errorHandlingPolicy,
            databaseChangesExecutor,
            settings.getRetries());
  }

  /**
   * Get the prepared statement builder
   *
   * @return a PreparedStatementBuilder
   */
  public PreparedStatementBuilder getStatementBuilder() {
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