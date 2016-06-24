package io.confluent.connect.jdbc.sink.dialect;

/**
 * Lists avaialbe RDBMS SQL dialect.
 */
public enum DbDialectTypeEnum {
  MYSQL("mysql"),
  ORACLE("oracle"),
  MSSQL("sqlserver"),
  SQLITE("sqlite"),
  NONE("none");
  private final String value;

  DbDialectTypeEnum(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
