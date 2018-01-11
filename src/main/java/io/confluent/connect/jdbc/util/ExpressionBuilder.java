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

import javax.xml.bind.DatatypeConverter;

public class ExpressionBuilder {

  public interface Expressable {
    void appendTo(
        ExpressionBuilder builder,
        boolean useQuotes
    );
  }

  public interface Transform<T> {
    void apply(
        ExpressionBuilder builder,
        T input
    );
  }

  public interface ListBuilder<T> {

    ListBuilder<T> delimitedBy(String delimiter);

    <R> ListBuilder<R> transformedBy(Transform<R> transform);

    ExpressionBuilder of(Iterable<? extends T> objects);

    ExpressionBuilder of(Iterable<? extends T> objects1, Iterable<? extends T> objects2);

    ExpressionBuilder of(
        Iterable<? extends T> objects1,
        Iterable<? extends T> objects2,
        Iterable<? extends T> objects3
    );
  }

  /**
   * Get a {@link Transform} that will surround the inputs with quotes.
   *
   * @return the transform; never null
   */
  public static Transform<String> quote() {
    return new Transform<String>() {
      @Override
      public void apply(
          ExpressionBuilder builder,
          String input
      ) {
        builder.appendIdentifierQuoted(input);
      }
    };
  }

  /**
   * Get a {@link Transform} that will quote just the column names.
   *
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNames() {
    return new Transform<ColumnId>() {
      @Override
      public void apply(
          ExpressionBuilder builder,
          ColumnId input
      ) {
        builder.appendIdentifierQuoted(input.name());
      }
    };
  }

  /**
   * Get a {@link Transform} that will quote just the column names and append the given string.
   *
   * @param appended the string to append after the quoted column names
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNamesWith(final String appended) {
    return new Transform<ColumnId>() {
      @Override
      public void apply(
          ExpressionBuilder builder,
          ColumnId input
      ) {
        builder.appendIdentifierQuoted(input.name());
        builder.append(appended);
      }
    };
  }

  /**
   * Get a {@link Transform} that will append a placeholder rather than each of the column names.
   *
   * @param str the string to output instead the each column name
   * @return the transform; never null
   */
  public static Transform<ColumnId> placeholderInsteadOfColumnNames(final String str) {
    return new Transform<ColumnId>() {
      @Override
      public void apply(
          ExpressionBuilder builder,
          ColumnId input
      ) {
        builder.append(str);
      }
    };
  }

  /**
   * Get a {@link Transform} that will append the prefix and then the quoted column name.
   *
   * @param prefix the string to output before the quoted column names
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNamesWithPrefix(final String prefix) {
    return new Transform<ColumnId>() {
      @Override
      public void apply(
          ExpressionBuilder builder,
          ColumnId input
      ) {
        builder.append(prefix);
        builder.appendIdentifierQuoted(input.name());
      }
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

  public ExpressionBuilder() {
    this(null);
  }

  public ExpressionBuilder(IdentifierRules rules) {
    this.rules = rules != null ? rules : IdentifierRules.DEFAULT;
  }

  /**
   * Return a new ExpressionBuilder that escapes quotes with the specified prefix.
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

  public ExpressionBuilder appendIdentifierDelimiter() {
    sb.append(rules.identifierDelimiter());
    return this;
  }

  public ExpressionBuilder appendLeadingQuote() {
    sb.append(rules.leadingQuoteString());
    return this;
  }

  public ExpressionBuilder appendTrailingQuote() {
    sb.append(rules.trailingQuoteString());
    return this;
  }

  public ExpressionBuilder appendStringQuote() {
    sb.append("'");
    return this;
  }

  public ExpressionBuilder appendStringQuoted(Object name) {
    appendStringQuote();
    sb.append(name);
    appendStringQuote();
    return this;
  }

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

  public ExpressionBuilder appendIdentifierQuoted(String name) {
    appendLeadingQuote();
    sb.append(name);
    appendTrailingQuote();
    return this;
  }

  public ExpressionBuilder appendBinaryLiteral(byte[] value) {
    return append("x'").append(DatatypeConverter.printHexBinary(value)).append("'");
  }

  public ExpressionBuilder appendNewLine() {
    sb.append(System.lineSeparator());
    return this;
  }

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

  public ExpressionBuilder append(Object obj) {
    return append(obj, true);
  }

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
      this.transform = transform != null ? transform : new Transform<T>() {
        @Override
        public void apply(
            ExpressionBuilder builder,
            T input
        ) {
          builder.append(input);
        }
      };
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

    @Override
    public ExpressionBuilder of(
        Iterable<? extends T> objects1,
        Iterable<? extends T> objects2
    ) {
      of(objects1);
      of(objects2);
      return ExpressionBuilder.this;
    }

    @Override
    public ExpressionBuilder of(
        Iterable<? extends T> objects1,
        Iterable<? extends T> objects2,
        Iterable<? extends T> objects3
    ) {
      of(objects1);
      of(objects2);
      of(objects3);
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
