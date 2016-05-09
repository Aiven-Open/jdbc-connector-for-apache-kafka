package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

public abstract class BasePreparedStatementBinder implements PreparedStatementBinder {
    private final String fieldName;

    protected BasePreparedStatementBinder(final String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }
}
