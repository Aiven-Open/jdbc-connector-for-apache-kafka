package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.util.Collection;

/**
 * The policy propagates the error further
 */
public final class ThrowErrorHandlingPolicy implements ErrorHandlingPolicy {
    @Override
    public void handle(Collection<SinkRecord> records, final Throwable error, final Connection connection) {
        throw new RuntimeException(error);
    }

}
