package io.confluent.connect.jdbc.sink.config;

/**
 * Holds the information for a field alias.
 */
public final class FieldAlias {
  private final boolean isPrimaryKey;
  private final String name;

  /**
   * Creates a new instance of FieldAlias
   *
   * @param name - The field alias name (or the actual field name)
   * @param isPrimaryKey - If true the field is part of the the table primary key
   */
  public FieldAlias(final String name, final boolean isPrimaryKey) {
    if (name == null || name.trim().length() == 0) {
      throw new IllegalArgumentException("<name> is not a valid argument.");
    }
    this.isPrimaryKey = isPrimaryKey;
    this.name = name;
  }

  /**
   * Creates a new instance of FieldAlias
   *
   * @param name - The field name
   */
  public FieldAlias(final String name) {
    this(name, false);
  }

  /**
   * Specifies if the field is part of the primary key
   *
   * @return - true - the field is part of the primary key; false - the field is not part of the primary key
   */
  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  /**
   * Returns the field alias.
   *
   * @return The field name
   */
  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldAlias that = (FieldAlias) o;

    if (isPrimaryKey != that.isPrimaryKey) {
      return false;
    }
    return name != null ? name.equals(that.name) : that.name == null;

  }

  @Override
  public int hashCode() {
    int result = (isPrimaryKey ? 1 : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "{name:" + name + "; isPrimaryKey:" + isPrimaryKey + "}";
  }
}
