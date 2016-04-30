package com.datamountaineer.streamreactor.connect;

import org.apache.kafka.connect.data.Struct;

import java.util.List;

public interface StructFieldsValuesExtractor {
    List<FieldNameAndValue> get(Struct struct);
}
