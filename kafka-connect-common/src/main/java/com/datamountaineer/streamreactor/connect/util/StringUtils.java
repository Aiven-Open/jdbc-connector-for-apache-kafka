package com.datamountaineer.streamreactor.connect.util;

/**
 * Helper class for String.
 */
public final class StringUtils {
    public static String join(Iterable<String> collection, String delimiter) {
        final StringBuilder builder = new StringBuilder();
        for (String item : collection) {
            if (builder.length() > 0)
                builder.append(delimiter);
            builder.append(item);
        }
        return builder.toString();
    }
}
