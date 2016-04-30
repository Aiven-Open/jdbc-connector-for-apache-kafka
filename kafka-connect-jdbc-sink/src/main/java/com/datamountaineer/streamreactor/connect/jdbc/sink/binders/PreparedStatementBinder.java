package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementBinder {
    void bind(int index, PreparedStatement statement) throws SQLException;
}


