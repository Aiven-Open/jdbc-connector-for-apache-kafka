/**
 * Copyright 2018 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc.util;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DropOptions;

/**
 * A class that can be used to build SQL expressions. A builder can be created with
 * {@link IdentifierRules} that dictate the quote characters and identifier delimiter,
 * or it can be obtained directly from a {@link DatabaseDialect}
 * via the {@link DatabaseDialect#expressionBuilder()} method.
 *
 * <p>The following is a simple example of how an instance of this class might be used to build a
 * SQL expression, where {@code table} might be a {@link TableId} object and {@code options} is a
 * {@link DropOptions} instance:
 *
 * <pre>
 *   builder.append("DROP TABLE ");
 *   builder.append(table);
 *   if (options.cascade()) {
 *     builder.append(" CASCADE CONSTRAINTS");
 *   }
 *   String dropStatement = builder.toString();
 * </pre>
 * The resulting drop statement expression might then be:
 * <pre>
 *   DROP TABLE "myCatalog"."myTable" CASCADE CONSTRAINTS
 * </pre>
 * Note how the {@link TableId} elements are properly quoted using the {@link IdentifierRules}
 * that are passed to the builder's constructor.
 *
 * <p>This class is often used within a {@link DatabaseDialect} implementation to construct the
 * various select, insert, update, upsert, and delete statements without having to explicitly
 * deal with fully-qualified identifiers, quoting rules, sizes of lists, etc.
 */
public class ExpressionBuilder {

  /**
   * A functional interface for anything that can be appended to an expression builder.
   * This makes use of double-dispatch to allow implementations to customize the behavior,
   * yet have callers not care about the differences in behavior.
   */
  @FunctionalInterface
  public interface Expressable {

    /**
     * Append this object to the specified builder.
     *
     * @param builder the builder to use; may not be null
     * @param useQuotes whether quotes should be used for this object
     */
    void appendTo(
        ExpressionBuilder builder,
        boolean useQuotes
    );
  }

  /**
   * A functional interface for a transformation that an expression builder might use when
   * appending one or more other objects.
   *
   * @param <T> the type of object to transform before appending.
   */
  @FunctionalInterface
  public interface Transform<T> {
    void apply(
        ExpressionBuilder builder,
        T input
    );
  }

  /**
   * A fluent API interface returned by the {@link ExpressionBuilder#appendList()} method that
   * allows a caller to easily define a custom delimiter to be used between items in the list,
   * an optional transformation that should be applied to each item in the list, and the
   * items in the list. This is very handle when the number of items is not known a priori.
   *
   * @param <T> the type of object to be appended to the expression builder
   */
  public interface ListBuilder<T> {

    /**
     * Define the delimiter to appear between items in the list. If not specified, a comma
     * is used as the default delimiter.
     *
     * @param delimiter the delimiter; may not be null
     * @return this builder to enable methods to be chained; never null
     */
    ListBuilder<T> delimitedBy(String delimiter);

    /**
     * Define a {@link Transform} that should be applied to every item in the list as it is
     * appended.
     *
     * @param transform the transform; may not be null
     * @return this builder to enable methods to be chained; never null
     * @param <R> the type of item to be transformed
     */
    <R> ListBuilder<R> transformedBy(Transform<R> transform);

    /**
     * Append to this list all of the items in the specified {@link Iterable}.
     *
     * @param objects the objects to be appended to the list
     * @return this builder to enable methods to be chained; never null
     */
    ExpressionBuilder of(Iterable<? extends T> objects);

    /**
     * Append to this list all of the items in the specified {@link Iterable} objects.
     *
     * @param objects1 the first collection of objects to be added to the list
     * @param objects2 a second collection of objects to be added to the list
     * @return this builder to enable methods to be chained; never null
     */
    default ExpressionBuilder of(Iterable<? extends T> objects1, Iterable<? extends T> objects2) {
      of(objects1);
      return of(objects2);
    }

    /**
     * Append to this list all of the items in the specified {@link Iterable} objects.
     *
     * @param objects1 the first collection of objects to be added to the list
     * @param objects2 a second collection of objects to be added to the list
     * @param objects3 a third collection of objects to be added to the list
     * @return this builder to enable methods to be chained; never null
     */
    default ExpressionBuilder of(
        Iterable<? extends T> objects1,
        Iterable<? extends T> objects2,
        Iterable<? extends T> objects3
    ) {
      of(objects1);
      of(objects2);
      return of(objects3);
    }
  }

  /**
   * Get a {@link Transform} that will surround the inputs with quotes.
   *
   * @return the transform; never null
   */
  public static Transform<String> quote() {
    return (builder, input) -> builder.appendIdentifierQuoted(input);
  }

  /**
   * Get a {@link Transform} that will quote just the column names.
   *
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNames() {
    return (builder, input) -> builder.appendIdentifierQuoted(input.name());
  }

  /**
   * Get a {@link Transform} that will quote just the column names and append the given string.
   *
   * @param appended the string to append after the quoted column names
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNamesWith(final String appended) {
    return (builder, input) -> {
      builder.appendIdentifierQuoted(input.name());
      builder.append(appended);
    };
  }

  /**
   * Get a {@link Transform} that will append a placeholder rather than each of the column names.
   *
   * @param str the string to output instead the each column name
   * @return the transform; never null
   */
  public static Transform<ColumnId> placeholderInsteadOfColumnNames(final String str) {
    return (builder, input) -> builder.append(str);
  }

  /**
   * Get a {@link Transform} that will append the prefix and then the quoted column name.
   *
   * @param prefix the string to output before the quoted column names
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNamesWithPrefix(final String prefix) {
    return (builder, input) -> {
      builder.append(prefix);
      builder.appendIdentifierQuoted(input.name());
    };
  }

  /**
   * Create a new ExpressionBuilder using the default {@link IdentifierRules}.
   *
   * @return the expression builder
   */
  public static ExpressionBuilder create() {
    return new ExpressionBuilder();
  }

  private final IdentifierRules rules;
  private final StringBuilder sb = new StringBuilder();

  /**
   * Create a new expression builder with the default {@link IdentifierRules}.
   */
  public ExpressionBuilder() {
    this(null);
  }

  /**
   * Create a new expression builder that uses the specified {@link IdentifierRules}.
   *
   * @param rules the rules; may be null if the default rules are to be used
   */
  public ExpressionBuilder(IdentifierRules rules) {
    this.rules = rules != null ? rules : IdentifierRules.DEFAULT;
  }

  /**
   * Return a new ExpressionBuilder that escapes quotes with the specified prefix.
   * This builder remains unaffected.
   *
   * @param prefix the prefix
   * @return the new ExpressionBuilder, or this builder if the prefix is null or empty
   */
  public ExpressionBuilder escapeQuotesWith(String prefix) {
    if (prefix == null || prefix.isEmpty()) {
      return this;
    }
    return new ExpressionBuilder(this.rules.escapeQuotesWith(prefix));
  }

  /**
   * Append to this builder's expression the delimiter defined by this builder's
   * {@link IdentifierRules}.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendIdentifierDelimiter() {
    sb.append(rules.identifierDelimiter());
    return this;
  }

  /**
   * Append to this builder's expression the leading quote character(s) defined by this builder's
   * {@link IdentifierRules}.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendLeadingQuote() {
    sb.append(rules.leadingQuoteString());
    return this;
  }

  /**
   * Append to this builder's expression the trailing quote character(s) defined by this builder's
   * {@link IdentifierRules}.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendTrailingQuote() {
    sb.append(rules.trailingQuoteString());
    return this;
  }

  /**
   * Append to this builder's expression the string quote character ({@code '}).
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendStringQuote() {
    sb.append("'");
    return this;
  }

  /**
   * Append to this builder's expression a string surrounded by single quote characters ({@code '}).
   *
   * @param name the object whose string representation is to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendStringQuoted(Object name) {
    appendStringQuote();
    sb.append(name);
    appendStringQuote();
    return this;
  }

  /**
   * Append to this builder's expression the identifier.
   *
   * @param name the name to be appended
   * @param quoted true if the name should be quoted, or false otherwise
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendIdentifier(
      String name,
      boolean quoted
  ) {
    if (quoted) {
      appendLeadingQuote();
    }
    sb.append(name);
    if (quoted) {
      appendTrailingQuote();
    }
    return this;
  }

  /**
   * Append to this builder's expression the specified identifier, surrounded by the leading and
   * trailing quotes.
   *
   * @param name the name to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendIdentifierQuoted(String name) {
    appendLeadingQuote();
    sb.append(name);
    appendTrailingQuote();
    return this;
  }

  /**
   * Append to this builder's expression the binary value as a hex string, prefixed and
   * suffixed by a single quote character.
   *
   * @param value the value to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendBinaryLiteral(byte[] value) {
    return append("x'").append(BytesUtil.toHex(value)).append("'");
  }

  /**
   * Append to this builder's expression a new line.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendNewLine() {
    sb.append(System.lineSeparator());
    return this;
  }

  /**
   * Append to this builder's expression the specified object. If the object is {@link Expressable},
   * then this builder delegates to the object's
   * {@link Expressable#appendTo(ExpressionBuilder, boolean)} method. Otherwise, the string
   * representation of the object is appended to the expression.
   *
   * @param obj the object to be appended
   * @param useQuotes true if the object should be surrounded by quotes, or false otherwise
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder append(
      Object obj,
      boolean useQuotes
  ) {
    if (obj instanceof Expressable) {
      ((Expressable) obj).appendTo(this, useQuotes);
    } else if (obj != null) {
      sb.append(obj);
    }
    return this;
  }

  /**
   * Append to this builder's expression the specified object surrounded by quotes. If the object
   * is {@link Expressable}, then this builder delegates to the object's
   * {@link Expressable#appendTo(ExpressionBuilder, boolean)} method. Otherwise, the string
   * representation of the object is appended to the expression.
   *
   * @param obj the object to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder append(Object obj) {
    return append(obj, true);
  }

  /**
   * Append to this builder's expression the specified object surrounded by quotes. If the object
   * is {@link Expressable}, then this builder delegates to the object's
   * {@link Expressable#appendTo(ExpressionBuilder, boolean)} method. Otherwise, the string
   * representation of the object is appended to the expression.
   *
   * @param obj the object to be appended
   * @param transform the transform that should be used on the supplied object to obtain the
   *                  representation that is appended to the expression; may be null
   *
   * @return this builder to enable methods to be chained; never null
   */
  public <T> ExpressionBuilder append(
      T obj,
      Transform<T> transform
  ) {
    if (transform != null) {
      transform.apply(this, obj);
    } else {
      append(obj);
    }
    return this;
  }

  protected class BasicListBuilder<T> implements ListBuilder<T> {
    private final String delimiter;
    private final Transform<T> transform;
    private boolean first = true;

    BasicListBuilder() {
      this(", ", null);
    }

    BasicListBuilder(String delimiter, Transform<T> transform) {
      this.delimiter = delimiter;
      this.transform = transform != null ? transform : ExpressionBuilder::append;
    }

    @Override
    public ListBuilder<T> delimitedBy(String delimiter) {
      return new BasicListBuilder<T>(delimiter, transform);
    }

    @Override
    public <R> ListBuilder<R> transformedBy(Transform<R> transform) {
      return new BasicListBuilder<>(delimiter, transform);
    }

    @Override
    public ExpressionBuilder of(Iterable<? extends T> objects) {
      for (T obj : objects) {
        if (first) {
          first = false;
        } else {
          append(delimiter);
        }
        append(obj, transform);
      }
      return ExpressionBuilder.this;
    }
  }

  public ListBuilder<Object> appendList() {
    return new BasicListBuilder<>();
  }

  public ExpressionBuilder appendMultiple(
      String delimiter,
      String expression,
      int times
  ) {
    for (int i = 0; i < times; i++) {
      if (i > 0) {
        append(delimiter);
      }
      append(expression);
    }
    return this;
  }

  public String toString() {
    return sb.toString();
  }
}
