package com.datamountaineer.streamreactor.connect.jdbc.common;

import com.datamountaineer.streamreactor.connect.jdbc.ConnectionProvider;

import java.sql.SQLException;

/**
 * To help with unit testing
 */
public interface DatabaseMetadataProvider {
  DatabaseMetadata get(final ConnectionProvider connectionProvider) throws SQLException;
}
