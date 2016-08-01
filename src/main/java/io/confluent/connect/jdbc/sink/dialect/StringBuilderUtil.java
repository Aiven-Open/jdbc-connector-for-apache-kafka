/*
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.jdbc.sink.dialect;

import java.util.Iterator;

class StringBuilderUtil {

  public interface Transform<T> {
    void apply(StringBuilder builder, T input);
  }

  public static void nCopiesToBuilder(StringBuilder builder, String delim, String item, int n) {
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        builder.append(delim);
      }
      builder.append(item);
    }
  }

  public static <T> void joinToBuilder(StringBuilder builder, String delim, Iterable<T> iter, Transform<T> transform) {
    joinToBuilder(builder, delim, iter, false, transform);
  }

  public static <T> void joinToBuilder(StringBuilder builder, String delim, Iterable<T> a, Iterable<T> b, Transform<T> transform) {
    final boolean updated = joinToBuilder(builder, delim, a, false, transform);
    joinToBuilder(builder, delim, b, updated, transform);
  }

  private static <T> boolean joinToBuilder(StringBuilder builder, String delim, Iterable<T> items, boolean startingDelim, Transform<T> transform) {
    boolean updated = false;
    Iterator<T> iter = items.iterator();
    if (iter.hasNext()) {
      if (startingDelim) {
        builder.append(delim);
      }
      T next = iter.next();
      if (next != null) {
        transform.apply(builder, next);
      }
      updated = true;
    }
    while (iter.hasNext()) {
      builder.append(delim);
      T next = iter.next();
      if (next != null) {
        transform.apply(builder, next);
      }
    }
    return updated;
  }

}
