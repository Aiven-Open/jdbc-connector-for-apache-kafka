package io.aiven.kafka.connect.jdbc.postgres;

import io.aiven.kafka.connect.jdbc.AbstractIT;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

public class RedshiftIT extends AbstractIT {
  protected void executeUpdate(final String updateStatement) throws SQLException {
    try (final Connection connection = getDatasource().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.executeUpdate(updateStatement);
    }
  }

  protected DataSource getDatasource() {
    final PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
    pgSimpleDataSource.setServerNames(new String[] {"admin.REPLACE.us-west-2.redshift-serverless.amazonaws.com"});
    pgSimpleDataSource.setPortNumbers(new int[] {5439});
    pgSimpleDataSource.setDatabaseName("dev");
    pgSimpleDataSource.setUser("REPLACE");
    pgSimpleDataSource.setPassword("REPLACE");
    return pgSimpleDataSource;
  }

  protected Map<String, String> basicConnectorConfig() {
    final HashMap<String, String> config = new HashMap<>();
    config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
    config.put("key.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
    config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    config.put("value.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
    config.put("tasks.max", "1");
    config.put("connection.url", "jdbc:redshift://admin.REPLACE.us-west-2.redshift-serverless.amazonaws.com:5439/dev?user=REPLACE&password=REPLACE");
    config.put("connection.user", "REPLACE");
    config.put("connection.password", "REPLACE");
    config.put("dialect.name", "RedshiftDatabaseDialect");
    return config;
  }

}
