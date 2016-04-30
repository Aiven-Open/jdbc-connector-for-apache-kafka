package com.datamountaineer.streamreactor.connect.sink;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Builds the new record key for the given connect SinkRecord.
 */
public interface StringKeyBuilder {
    String build(SinkRecord record);
}
