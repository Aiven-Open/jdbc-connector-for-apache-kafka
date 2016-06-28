package io.confluent.connect.jdbc.sink.common;

import java.sql.SQLException;

import io.confluent.connect.jdbc.sink.ConnectionProvider;

public interface DatabaseMetadataProvider {
  DatabaseMetadata get(final ConnectionProvider connectionProvider) throws SQLException;
}
