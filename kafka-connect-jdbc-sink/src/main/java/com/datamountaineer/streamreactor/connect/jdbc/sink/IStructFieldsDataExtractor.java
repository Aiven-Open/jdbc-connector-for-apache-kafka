package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.Pair;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import org.apache.kafka.connect.data.Struct;

import java.util.List;

public interface IStructFieldsDataExtractor {
    List<Pair<String, PreparedStatementBinder>> get(final Struct struct);
}
