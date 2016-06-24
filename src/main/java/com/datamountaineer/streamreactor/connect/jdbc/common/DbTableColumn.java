package com.datamountaineer.streamreactor.connect.jdbc.common;

/**
 * Holds the information about a database table column.
 */
public final class DbTableColumn {
  private final boolean isPrimaryKey;
  private final boolean allowsNull;
  private final int sqlType;
  private final String name;

  /**
   * Creates a new instance of DbTableColumn
   *
   * @param name         - The column name
   * @param isPrimaryKey - true if the column is a primary key
   * @param allowsNull   - true if the column allows null
   * @param sqlType      - The actual database type (not used for now)
   */
  public DbTableColumn(final String name, final boolean isPrimaryKey, final boolean allowsNull, final int sqlType) {
    this.isPrimaryKey = isPrimaryKey;
    this.allowsNull = allowsNull;
    this.name = name;
    this.sqlType = sqlType;
  }

  /**
   * Gets the column name
   *
   * @return The column name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns true if the column allows null; false- otherwise
   *
   * @return Returns true if the column allows null; false- otherwise
   */
  public boolean allowsNull() {
    return allowsNull;
  }

  /**
   * Returns true if the column is a primary key; false- otherwise
   *
   * @return true if the column is a primary key; false - otherwise
   */
  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  /**
   * Returns the database column type
   *
   * @return The database column type
   */
  public int getSqlType() {
    return sqlType;
  }

  @Override
  public String toString() {
    return String.format("{name:%s,isPrimaryKey:%s}", name, String.valueOf(isPrimaryKey));
  }
}