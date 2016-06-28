package io.confluent.connect.jdbc.sink.writer;

import java.util.List;

import io.confluent.connect.jdbc.sink.binders.PreparedStatementBinder;


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
