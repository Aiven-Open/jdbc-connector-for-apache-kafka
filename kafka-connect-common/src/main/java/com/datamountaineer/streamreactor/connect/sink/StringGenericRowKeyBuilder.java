package com.datamountaineer.streamreactor.connect.sink;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Uses the connect record (topic, partition, offset) to build the record key
 */
public final class StringGenericRowKeyBuilder implements StringKeyBuilder {

    private final String keyDelimiter;

    public StringGenericRowKeyBuilder(String keyDelimiter) {
        this.keyDelimiter = keyDelimiter;
    }

    @Override
    public String build(SinkRecord record) {
        String builder = record.topic() +
                keyDelimiter +
                record.kafkaPartition() +
                keyDelimiter +
                record.kafkaOffset();

        return builder;
    }
}
