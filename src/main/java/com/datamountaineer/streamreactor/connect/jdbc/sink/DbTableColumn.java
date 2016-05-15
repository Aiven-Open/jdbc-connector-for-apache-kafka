/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

/**
 * Holds the information about a database table column.
 */
public final class DbTableColumn {
  private final boolean isPrimaryKey;
  private final boolean allowsNull;
  private final int sqlType;
  private final String name;

  public DbTableColumn(final String name, final boolean isPrimaryKey, final boolean allowsNull, final int sqlType) {
    this.isPrimaryKey = isPrimaryKey;
    this.allowsNull = allowsNull;
    this.name = name;
    this.sqlType = sqlType;
  }

  public String getName() {
    return name;
  }

  public boolean allowsNull() {
    return allowsNull;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public int getSqlType() {
    return sqlType;
  }
}
