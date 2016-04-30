package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.sink.DbWriter;
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
import java.util.List;

/**
 * Responsible for taking a sequence of SinkRecord and write them to the database
 */
public final class JdbcDbWriter implements DbWriter {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDbWriter.class);

    private final PreparedStatementBuilder statementBuilder;
    private final ErrorHandlingPolicy errorHandlingPolicy;

    //provides connection pooling
    private final HikariDataSource dataSource;

    /**
     * @param connectionStr       - The database connection string
     * @param statementBuilder    - Returns a sequence of PreparedStatement to process
     * @param errorHandlingPolicy - An instance of the error handling approach
     */
    public JdbcDbWriter(String connectionStr, PreparedStatementBuilder statementBuilder, ErrorHandlingPolicy errorHandlingPolicy) {

        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connectionStr);
//config.setUsername("bart");
//config.setPassword("51mp50n");
        this.dataSource = new HikariDataSource(config);
        this.statementBuilder = statementBuilder;
        this.errorHandlingPolicy = errorHandlingPolicy;
    }
    //val connection = DriverManager.getConnection(connectionStr)

    /**
     * Writes the given records to the database
     *
     * @param records - The sequence of records to insert
     */
    @Override
    public void write(final Collection<SinkRecord> records) {
        if (records.isEmpty())
            logger.warn("Received empty sequence of SinkRecord");
        else {

            Connection connection = null;
            List<PreparedStatement> statements = null;
            try {
                connection = dataSource.getConnection();
                statements = statementBuilder.build(records, connection);
                if (!statements.isEmpty()) {
                    //begin transaction
                    connection.setAutoCommit(false);
                    for (final PreparedStatement statement : statements) {
                        statement.execute();
                    }
                    //commit the transaction
                    connection.commit();

                }
            } catch (SQLException sqlException) {
                final SinkRecord firstRecord = Iterators.getNext(records.iterator(), null);
                logger.error(String.format("Following error has occurred inserting data starting at topic:%s offset:%d partition:%d",
                        firstRecord.topic(),
                        firstRecord.kafkaOffset(),
                        firstRecord.kafkaPartition()));

                if (connection != null) {//rollback the transaction
                    try {
                        connection.rollback();
                    } catch (Throwable t) {
                    }
                }

                //handle the exception
                errorHandlingPolicy.handle(records, sqlException, connection);
            } finally {
                if (statements != null) {
                    for (final PreparedStatement statement : statements) {
                        try {
                            statement.close();
                        } catch (Throwable t) {
                        }
                    }
                }

                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Throwable t) {
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
     * @param settings - Holds the sink settings
     * @return Returns a new instsance of JdbcDbWriter
     */
    public static JdbcDbWriter from(final JdbcSinkSettings settings) {

        final PreparedStatementBuilder statementBuilder = PreparedStatementBuilderHelper.from(settings);
        final ErrorHandlingPolicy errorHandlingPolicy = ErrorHandlingPolicyHelper.from(settings.getErrorPolicy());

        return new JdbcDbWriter(settings.getConnection(), statementBuilder, errorHandlingPolicy);
    }

    public PreparedStatementBuilder getStatementBuilder() {
        return statementBuilder;
    }

    public ErrorHandlingPolicy getErrorHandlingPolicy() {
        return errorHandlingPolicy;
    }
}