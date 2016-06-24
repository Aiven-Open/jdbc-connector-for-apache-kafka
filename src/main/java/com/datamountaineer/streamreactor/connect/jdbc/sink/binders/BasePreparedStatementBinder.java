package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

public abstract class BasePreparedStatementBinder implements PreparedStatementBinder {
  private boolean isPrimaryKey = false;
  private final String fieldName;

  BasePreparedStatementBinder(final String fieldName) {
    this.fieldName = fieldName;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public void setPrimaryKey(boolean value) {
    isPrimaryKey = value;
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }
}
