/**
 * Copyright 2016 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.jdbc.sink.common;

public class ParameterValidator {
  /**
   * Checks the given value is not null or empty. If the value doesn't meet the criteria an IllegalArgumentException is
   * thrown
   *
   * @param value     - The value to be checked
   * @param paramName - The parameter name of the caller function
   */
  public static void notNullOrEmpty(final String value, final String paramName) {
    if (value == null || value.trim().length() == 0) {
      throw new IllegalArgumentException(String.format("%s is not set correctly.", paramName));
    }
  }

  /**
   * Checks the give obj is not null. If the value provided is null an IllegalArgumentException is thrown.
   * @param obj
   * @param paramName
   */
  public static void notNull(final Object obj, final String paramName) {
    if (obj == null) {
      throw new IllegalArgumentException(String.format("%s is not set correctly.", paramName));
    }
  }
}
