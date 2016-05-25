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


package com.datamountaineer.streamreactor.connect.jdbc.dialect;

import com.datamountaineer.streamreactor.connect.jdbc.common.ParameterValidator;
import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;
import com.google.common.base.Joiner;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Describes which SQL dialect to use. Different databases support different syntax for upserts.
 */
public abstract class DbDialect {

  private final Map<Schema.Type, String> schemaTypeToSqlTypeMap;
  protected final String escapeColumnNamesStart;
  protected final String escapeColumnNamesEnd;

  DbDialect(Map<Schema.Type, String> schemaTypeToSqlTypeMap, String escapeColumnNamesStart, String escapeColumnNamesEnd) {
    this.escapeColumnNamesStart = escapeColumnNamesStart;
    this.escapeColumnNamesEnd = escapeColumnNamesEnd;
    ParameterValidator.notNull(schemaTypeToSqlTypeMap, "schemaTypeToSqlTypeMap");
    this.schemaTypeToSqlTypeMap = schemaTypeToSqlTypeMap;
  }

  /**
   * Gets the query allowing to insert a new row into the RDBMS even if it does previously exists
   *
   * @param table       - Contains the name of the target table
   * @param columns     - Contains the table non primary key columns which will get data inserted in
   * @param keyColumns- Contains the table primary key columns
   * @return The upsert query for the dialect
   */
  public abstract String getUpsertQuery(final String table,
                                        final List<String> columns,
                                        final List<String> keyColumns);


  /**
   * Maps a JDBC  URI to an instance of a derived class of DbDialect
   *
   * @param connection - The jdbc connection uri
   * @return - An instance of DbDialect
   */
  public static DbDialect fromConnectionString(final String connection) {
    ParameterValidator.notNullOrEmpty(connection, "connection");
    if (!connection.startsWith("jdbc:")) {
      throw new IllegalArgumentException("connection is not valid. Expecting a jdbc uri: jdbc:protocol//server:port/...");
    }

//sqlite URIs are not in the format jdbc:protocol://FILE but jdbc:protocol:file
    if (connection.startsWith("jdbc:sqlite:")) {
      return new SQLiteDialect();
    }

    if (connection.startsWith("jdbc:oracle:thin:@")) {
      return new OracleDialect();
    }

    final String protocol = extractProtocol(connection).toLowerCase();
    switch (protocol) {
      case "microsoft:sqlserver":
        return new SqlServerDialect();

      case "mariadb":
        return new MariaDialect();

      case "mysql":
        return new MySqlDialect();

      case "postgresql":
        return new PostgreSQLDialect();

      default:
        throw new IllegalArgumentException(String.format("%s jdbc is not handled.", protocol));
    }
  }


  /**
   * Returns the query for creating a new table in the database
   *
   * @param tableName
   * @param fields
   * @return The create query for the dialect
   */
  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is not valid.Not accepting empty collection of fields.");
    }
    final StringBuilder builder = new StringBuilder();
    builder.append(String.format("CREATE TABLE %s (", handleTableName(tableName)));
    boolean first = true;

    List<String> pks = new ArrayList<>();
    for (final SinkRecordField f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(System.lineSeparator());
      builder.append(escapeColumnNamesStart);
      builder.append(f.getName());
      builder.append(escapeColumnNamesEnd);
      builder.append(" ");

      if (f.isPrimaryKey() && f.getType().equals(Schema.Type.STRING)) {
        builder.append("VARCHAR(50)");
      } else {
        builder.append(getSqlType(f.getType()));
      }

      if (f.isPrimaryKey()) {
        builder.append(" NOT NULL");
        pks.add(escapeColumnNamesStart + f.getName() + escapeColumnNamesEnd);
      } else {
        builder.append(" NULL");
      }
    }
    if (pks.size() > 0) {
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("PRIMARY KEY(");
      builder.append(Joiner.on(",").join(pks));
      builder.append(")");
    }
    builder.append(");");
    return builder.toString();
  }

  /**
   * Returns the query to alter a table by adding a new table
   *
   * @param tableName
   * @param fields
   * @return The alter query for the dialect
   */
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    ParameterValidator.notNullOrEmpty(tableName, "table");
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is empty.");
    }
    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(handleTableName(tableName));
    boolean first = true;
    for (final SinkRecordField f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(System.lineSeparator());
      builder.append("ADD COLUMN ");
      builder.append(escapeColumnNamesStart);
      builder.append(f.getName());
      builder.append(escapeColumnNamesEnd);
      builder.append(" ");
      builder.append(getSqlType(f.getType()));
      builder.append(" NULL");
    }
    builder.append(";");

    final List<String> query = new ArrayList<String>(1);
    query.add(builder.toString());
    return query;
  }

  /**
   * Maps the Schema type to a database data type.
   *
   * @param type
   * @return The sqlType for the dialect
   */
  String getSqlType(Schema.Type type) {
    final String sqlType = schemaTypeToSqlTypeMap.get(type);
    if (sqlType == null) {
      throw new IllegalArgumentException(String.format("%s type doesn't have a mapping for SQL database column type",
              type.toString()));
    }
    return sqlType;
  }

  protected String handleTableName(String tableName) {
    return tableName;
  }

  /**
   * Extracts the database protocol from a jdbc URI.
   *
   * @param connection
   * @return The sql protocol
   */
  public static String extractProtocol(final String connection) {
    ParameterValidator.notNullOrEmpty(connection, "connection");
    if (!connection.startsWith("jdbc:"))
      throw new IllegalArgumentException("connection is not a valid jdbc URI");

    int index = connection.indexOf("://", "jdbc:".length());
    if (index < 0) {
      throw new IllegalArgumentException(String.format("%s is not a valid jdbc uri.", connection));
    }
    return connection.substring("jdbc:".length(), index);
  }
}