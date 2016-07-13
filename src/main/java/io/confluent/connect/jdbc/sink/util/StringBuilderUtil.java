package io.confluent.connect.jdbc.sink.util;

import java.util.Iterator;

public class StringBuilderUtil {

  public interface Transform<T> {
    void apply(StringBuilder builder, T input);
  }

  public static Transform<String> stringSurroundTransform(final String start, final String end) {
    return new Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String input) {
        builder.append(start).append(input).append(end);
      }
    };
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
