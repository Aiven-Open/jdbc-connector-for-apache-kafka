package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.google.common.base.Objects;

/**
 * Holds the information for a field alias.
 */
public final class FieldAlias {
  private final boolean isPrimaryKey;
  private final String name;

  /**
   * Creates a new instance of FieldAlias
   *
   * @param name         - The field alias name (or the actual field name)
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

  /**
   * Checks the two objects for equality by delegating to their respective
   * {@link Object#equals(Object)} methods.
   *
   * @param o the {@link FieldAlias} to which this one is to be checked for equality
   *          equal
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof FieldAlias)) {
      return false;
    }
    final FieldAlias fa = (FieldAlias) o;
    return Objects.equal(name, fa.name) && isPrimaryKey == fa.isPrimaryKey;
  }

  /**
   * Compute a hash code using the hash codes of the underlying objects
   *
   * @return a hashcode of the FieldAlias
   */
  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "{name:" + name + "; isPrimaryKey:" + isPrimaryKey + "}";
  }
}
