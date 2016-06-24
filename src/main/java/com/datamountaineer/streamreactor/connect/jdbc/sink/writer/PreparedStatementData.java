package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;

import java.util.List;


public final class PreparedStatementData {
  private final String sql;
  private final List<Iterable<PreparedStatementBinder>> binders;

  public PreparedStatementData(String sql, List<Iterable<PreparedStatementBinder>> binders) {
    this.sql = sql;
    this.binders = binders;
  }

  public String getSql() {
    return sql;
  }

  public List<Iterable<PreparedStatementBinder>> getBinders() {
    return binders;
  }

  public void addEntryBinders(Iterable<PreparedStatementBinder> entryBinders) {
    binders.add(entryBinders);
  }
}
