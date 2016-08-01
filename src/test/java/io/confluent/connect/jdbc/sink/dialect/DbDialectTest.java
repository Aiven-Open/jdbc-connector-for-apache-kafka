/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DbDialectTest {
  @Test(expected = ConnectException.class)
  public void shouldThrowAndExceptionIfTheUriDoesNotStartWithJdbcWhenExtractingTheProtocol() {
    DbDialect.extractProtocolFromUrl("mysql://Server:port");
  }

  @Test
  public void handleSqlServerJTDS() {
    String[] conns = new String[]{
        "jdbc:sqlserver://what.amazonaws.com:1433/jdbc_sink_01",
        "jdbc:jtds:sqlserver://localhost;instance=SQLEXPRESS;DatabaseName=jdbc_sink_01"
    };
    for (String c : conns) {
      assertEquals(SqlServerDialect.class, DbDialect.fromConnectionString(c).getClass());
    }
  }

  @Test
  public void formatColumnValue() {
    verifyColumnValueConversion("42", Schema.Type.INT8, (byte) 42);
    verifyColumnValueConversion("42", Schema.Type.INT16, (short) 42);
    verifyColumnValueConversion("42", Schema.Type.INT32, 42);
    verifyColumnValueConversion("42", Schema.Type.INT64, 42L);
    verifyColumnValueConversion("42.5", Schema.Type.FLOAT32, 42.5f);
    verifyColumnValueConversion("42.5", Schema.Type.FLOAT64, 42.5d);
    verifyColumnValueConversion("0", Schema.Type.BOOLEAN, false);
    verifyColumnValueConversion("1", Schema.Type.BOOLEAN, true);
    verifyColumnValueConversion("'quoteit'", Schema.Type.STRING, "quoteit");
    verifyColumnValueConversion("x'2A'", Schema.Type.BYTES, new byte[]{42});
  }

  private void verifyColumnValueConversion(String expected, Schema.Type type, Object value) {
    final StringBuilder builder = new StringBuilder();
    DbDialect.formatColumnValue(builder, type, value);
    assertEquals(expected, builder.toString());
  }

  @Test(expected = ConnectException.class)
  public void shouldThrowAndExceptionIfTheUriDoesntHaveSemiColonAndForwardSlashWhenExtractingTheProtocol() {
    DbDialect.extractProtocolFromUrl("jdbc:protocol:somethingelse;field=value;");
  }

  @Test
  public void shouldExtractTheProtocol() {
    assertEquals(DbDialect.extractProtocolFromUrl("jdbc:protocol_test://SERVER:21421;field=value"), "protocol_test");
  }

  @Test
  public void getTheSqLiteDialect() {
    assertEquals(DbDialect.fromConnectionString("jdbc:sqlite:/folder/db.file").getClass(), SqliteDialect.class);
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
    assertEquals(DbDialect.fromConnectionString("jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE").getClass(), SqlServerDialect.class);
  }

  @Test
  public void getPostgreDialect() {
    assertEquals(DbDialect.fromConnectionString("jdbc:postgresql://HOST:1433;DatabaseName=DATABASE").getClass(), PostgreSqlDialect.class);
  }

  @Test
  public void getGenericDialect() {
    assertEquals(DbDialect.fromConnectionString("jdbc:other://host:42").getClass(), GenericDialect.class);
  }
}
