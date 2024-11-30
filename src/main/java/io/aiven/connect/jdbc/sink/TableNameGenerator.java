package io.aiven.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TableNameGenerator {
    private static final Pattern NORMALIZE_TABLE_NAME_FOR_TOPIC = Pattern.compile("[^a-zA-Z0-9_]");

    public static String generateTableName(JdbcSinkConfig config, String topic) {
        String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (config.tableNameNormalize) {
            tableName = NORMALIZE_TABLE_NAME_FOR_TOPIC.matcher(tableName).replaceAll("_");
        }
        if (!config.topicsToTablesMapping.isEmpty()) {
            tableName = config.topicsToTablesMapping.getOrDefault(topic, "");
        }
        if (tableName.isEmpty()) {
            throw new ConnectException(String.format(
                    "Destination table for the topic: '%s' couldn't be found in the topics to tables mapping: '%s' "
                            + "and couldn't be generated for the format string '%s'",
                    topic,
                    config.topicsToTablesMapping.entrySet().stream()
                            .map(e -> String.join("->", e.getKey(), e.getValue()))
                            .collect(Collectors.joining(",")),
                    config.tableNameFormat
            ));
        }
        return tableName;
    }
}
