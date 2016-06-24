package com.datamountaineer.streamreactor.connect.jdbc.common;

import com.datamountaineer.streamreactor.connect.jdbc.ConnectionProvider;

/**
 * To help with unit testing
 */
public interface DatabaseMetadataProvider {
  DatabaseMetadata get(final ConnectionProvider connectionProvider);
}
