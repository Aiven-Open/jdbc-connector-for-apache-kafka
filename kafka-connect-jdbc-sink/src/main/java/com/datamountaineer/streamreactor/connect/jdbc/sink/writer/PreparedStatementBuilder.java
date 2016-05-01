package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;


import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public interface PreparedStatementBuilder {
    List<PreparedStatement> build(final Collection<SinkRecord> records, final Connection connection) throws SQLException;

    boolean isBatching();
}

final class PreparedStatementBuilderHelper {
    /**
     * Creates a new instance of PrepareStatementBuilder
     *
     * @param settings - Instance of the Jdbc sink settings
     * @return - Returns an instance of PreparedStatementBuilder depending on the settings asking for batched or
     * non-batched inserts
     */
    public static PreparedStatementBuilder from(final JdbcSinkSettings settings) {
        final StructFieldsDataExtractor fieldsValuesExtractor = new StructFieldsDataExtractor(settings.getFields().getIncludeAllFields(),
                settings.getFields().getFieldsMappings());
        if (settings.isBatching()) {
            return new BatchedPreparedStatementBuilder(settings.getTableName(), fieldsValuesExtractor);
        }

        return new SinglePreparedStatementBuilder(settings.getTableName(), fieldsValuesExtractor);
    }
}


