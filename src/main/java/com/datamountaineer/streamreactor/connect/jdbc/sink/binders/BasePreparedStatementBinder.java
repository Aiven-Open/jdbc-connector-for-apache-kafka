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

package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

public abstract class BasePreparedStatementBinder implements PreparedStatementBinder {
  private boolean isPrimaryKey = false;
  private final String fieldName;

  BasePreparedStatementBinder(final String fieldName) {
    this.fieldName = fieldName;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public void setPrimaryKey(boolean value) {
    isPrimaryKey = value;
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }
}
