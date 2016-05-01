package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.Pair;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Creates a sql statement for each record
 */
public final class SinglePreparedStatementBuilder implements PreparedStatementBuilder {

    private static final Logger logger = LoggerFactory.getLogger(SinglePreparedStatementBuilder.class);

    private final String tableName;
    private final StructFieldsDataExtractor fieldsExtractor;

    /**
     * @param tableName       - The name of the database tabled
     * @param fieldsExtractor - An instance of the SinkRecord fields value extractor
     */
    public SinglePreparedStatementBuilder(String tableName, StructFieldsDataExtractor fieldsExtractor) {
        this.tableName = tableName;
        this.fieldsExtractor = fieldsExtractor;
    }

    /**
     * Creates a PreparedStatement for each SinkRecord
     *
     * @param records    - The sequence of records to be inserted to the database
     * @param connection - The database connection instance
     * @return A sequence of PreparedStatement to be executed. It will batch the sql operation.
     */
    @Override
    public List<PreparedStatement> build(final Collection<SinkRecord> records, final Connection connection) throws SQLException {
        final List<PreparedStatement> statements = new ArrayList<>(records.size());
        for (final SinkRecord record : records) {


            logger.debug("Received record from topic:%s partition:%d and offset:$d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            if (record.value() == null || record.value().getClass() != Struct.class)
                throw new IllegalArgumentException("The SinkRecord payload should be of type Struct");

            final List<Pair<String, PreparedStatementBinder>> fieldsAndBinders = fieldsExtractor.get((Struct) record.value());

            if (!fieldsAndBinders.isEmpty()) {
                final List<String> columns = Lists.transform(fieldsAndBinders, new Function<Pair<String, PreparedStatementBinder>, String>() {
                    @Override
                    public String apply(Pair<String, PreparedStatementBinder> input) {
                        return input.first;
                    }
                });

                final List<PreparedStatementBinder> binders = Lists.transform(fieldsAndBinders, new Function<Pair<String, PreparedStatementBinder>, PreparedStatementBinder>() {
                    @Override
                    public PreparedStatementBinder apply(Pair<String, PreparedStatementBinder> input) {
                        return input.second;
                    }
                });


                final String query = BuildInsertQuery.get(tableName, columns);
                final PreparedStatement statement = connection.prepareStatement(query);
                PreparedStatementBindData.apply(statement, binders);
                statements.add(statement);
            }
        }

        return statements;
    }

    @Override
    public boolean isBatching() {
        return false;
    }
}
