/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.connect.jdbc.config;

import org.apache.kafka.common.config.ConfigDef;

public class JdbcConfig {
  public static final String SQL_QUOTE_IDENTIFIERS_CONFIG = "sql.quote.identifiers";
  public static final ConfigDef.Type SQL_QUOTE_IDENTIFIERS_TYPE = ConfigDef.Type.BOOLEAN;
  public static final Boolean SQL_QUOTE_IDENTIFIERS_DEFAULT = true;
  public static final String SQL_QUOTE_IDENTIFIERS_DOC =
      "Whether to delimit (in most databases, quote with double quotes) identifiers "
          + "(e.g., table names and column names) in SQL statements.";
  public static final String SQL_QUOTE_IDENTIFIERS_DISPLAY = "Quote Identifiers";
}
