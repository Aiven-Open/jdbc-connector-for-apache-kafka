package io.confluent.connect.jdbc.sink.metadata;

public final class DbTableColumn {
  public final String name;
  public final boolean isPrimaryKey;
  public final boolean allowsNull;
  public final int sqlType;

  public DbTableColumn(final String name, final boolean isPrimaryKey, final boolean allowsNull, final int sqlType) {
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
    this.allowsNull = allowsNull;
    this.sqlType = sqlType;
  }

  @Override
  public String toString() {
    return "DbTableColumn{" +
           "name='" + name + '\'' +
           ", isPrimaryKey=" + isPrimaryKey +
           ", allowsNull=" + allowsNull +
           ", sqlType=" + sqlType +
           '}';
  }
}