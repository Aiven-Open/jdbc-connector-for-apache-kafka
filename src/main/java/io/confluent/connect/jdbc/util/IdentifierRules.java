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

public class IdentifierRules {

  private static final String DEFAULT_QUOTE = "\"";
  private static final String DEFAULT_ID_DELIM = ".";

  public static final IdentifierRules DEFAULT = new IdentifierRules(DEFAULT_ID_DELIM,
                                                                    DEFAULT_QUOTE
  );

  private final String leadingQuoteString;
  private final String trailingQuoteString;
  private final String identifierDelimiter;

  public IdentifierRules(String quoteString) {
    this(DEFAULT_ID_DELIM, quoteString, quoteString);
  }

  public IdentifierRules(
      String delimiter,
      String quoteString
  ) {
    this(delimiter, quoteString, quoteString);
  }

  public IdentifierRules(
      String identifierDelimiter,
      String leadingQuoteString,
      String trailingQuoteString
  ) {
    this.leadingQuoteString = leadingQuoteString != null ? leadingQuoteString : DEFAULT_QUOTE;
    this.trailingQuoteString = trailingQuoteString != null ? trailingQuoteString : DEFAULT_QUOTE;
    this.identifierDelimiter = identifierDelimiter != null ? identifierDelimiter : DEFAULT_ID_DELIM;
  }

  public String identifierDelimiter() {
    return identifierDelimiter;
  }

  public String leadingQuoteString() {
    return leadingQuoteString;
  }

  public String trailingQuoteString() {
    return trailingQuoteString;
  }

  public ExpressionBuilder expressionBuilder() {
    return new ExpressionBuilder(this);
  }

}
