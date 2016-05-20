package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DbDialectTest {
  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowAndExceptionIfTheUriDoesNotStartWithJdbcWhenExtractingTheProtocol() {
    DbDialect.extractProtocol("mysql://Server:port");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowAndExceptionIfTheUriIsNullWhenExtractingTheProtocol() {
    DbDialect.extractProtocol(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowAndExceptionIfTheUriIsEmptyWhenExtractingTheProtocol() {
    DbDialect.extractProtocol("   ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowAndExceptionIfTheUriDoesntHaveSemiColonAndForwardSlashWhenExtractingTheProtocol() {
    DbDialect.extractProtocol("jdbc:protocol:somethingelse;field=value;");
  }

  @Test
  public void shouldExtractTheProtocol() {
    assertEquals(DbDialect.extractProtocol("jdbc:protocol_test://SERVER:21421;field=value"), "protocol_test");
  }

  @Test
  public void getTheSqLiteDialect() {
    assertEquals(DbDialect.fromConnectionString("jdbc:sqlite:/folder/db.file").getClass(), SQLiteDialect.class);
  }

  @Test
  public void getSql2003DialectForOracle() {
    assertEquals(DbDialect.fromConnectionString("jdbc:oracle:thin:@localhost:1521:xe").getClass(), OracleDialect.class);
  }

  @Test
  public void getMySqlDialect() {
    assertEquals(DbDialect.fromConnectionString("jdbc:mysql://HOST/DATABASE").getClass(), MySqlDialect.class);
  }

  @Test
  public void getSqlServerDialect() {
    assertEquals(DbDialect.fromConnectionString("jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE").getClass(),
            SqlServerDialect.class);
  }

  @Test
  public void getPostgreDialect() {
    assertEquals(DbDialect.fromConnectionString("jdbc:postgresql://HOST:1433;DatabaseName=DATABASE").getClass(),
        PostgreSQLDialect.class);
  }
}
