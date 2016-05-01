package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class BytesPreparedStatementBinder implements PreparedStatementBinder {
    private final byte[] value;

    public BytesPreparedStatementBinder(byte[] value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setBytes(index, value);
    }


    public byte[] getValue(){
        return value;
    }
}
