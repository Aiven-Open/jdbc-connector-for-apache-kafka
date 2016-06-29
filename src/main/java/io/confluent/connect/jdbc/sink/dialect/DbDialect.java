package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.SinkRecordField;
import io.confluent.connect.jdbc.sink.common.ParameterValidator;

import static io.confluent.connect.jdbc.sink.common.StringBuilderUtil.*;

/**
 * Describes which SQL dialect to use. Different databases support different syntax for upserts.
 */
public abstract class DbDialect {

  private final Map<Schema.Type, String> schemaTypeToSqlTypeMap;
  protected final String escapeColumnNamesStart;
  protected final String escapeColumnNamesEnd;
  protected final String lineSeparator = System.lineSeparator();

  DbDialect(Map<Schema.Type, String> schemaTypeToSqlTypeMap, String escapeColumnNamesStart, String escapeColumnNamesEnd) {
    this.escapeColumnNamesStart = escapeColumnNamesStart;
    this.escapeColumnNamesEnd = escapeColumnNamesEnd;
    ParameterValidator.notNull(schemaTypeToSqlTypeMap, "schemaTypeToSqlTypeMap");
    this.schemaTypeToSqlTypeMap = schemaTypeToSqlTypeMap;
  }

  /**
   * Returns the create SQL statement
   *
   * @param tableName - The name of the table
   * @param nonKeyColumns - The sequence of non primary key columns
   * @param keyColumns - The sequence of primary key columns
   * @return SQL create statement
   */
  public final String getInsert(final String tableName,
                                final List<String> nonKeyColumns,
                                final List<String> keyColumns) {
    if (tableName == null || tableName.trim().length() == 0) {
      throw new IllegalArgumentException("tableName parameter is not a valid table name.");
    }
    if (nonKeyColumns == null) {
      throw new IllegalArgumentException("nonKeyColumns parameter is null.");
    }

    if (keyColumns == null) {
      throw new IllegalArgumentException("keyColumns parameter is null");
    }

    if (nonKeyColumns.isEmpty() && keyColumns.isEmpty()) {
      throw new IllegalArgumentException("Illegal arguments. Both nonKeyColumns and keyColumns are empty");
    }

    StringBuilder builder = new StringBuilder("INSERT INTO ");
    builder.append(handleTableName(tableName));
    builder.append("(");
    joinToBuilder(builder, ",", nonKeyColumns, keyColumns, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") VALUES(");
    joinToBuilder(builder, ",", Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"), stringIdentityTransform());
    builder.append(")");
    return builder.toString();
  }

  /**
   * Gets the query allowing to insert a new row into the RDBMS even if it does previously exists
   *
   * @param table - Contains the name of the target table
   * @param columns - Contains the table non primary key columns which will get data inserted in
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
      case "sqlserver":
      case "jtds:sqlserver":
        return new SqlServerDialect();

      case "mariadb":
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
   * @param tableName - The table name
   * @param fields - List of table columns
   * @return The create query for the dialect
   */
  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is not valid.Not accepting empty collection of fields.");
    }

    final StringBuilder builder = new StringBuilder();
    builder.append("CREATE TABLE ");
    builder.append(handleTableName(tableName));
    builder.append(" (");

    joinToBuilder(builder, ",", fields, new Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField f) {
        builder.append(lineSeparator);
        builder.append(escapeColumnNamesStart).append(f.getName()).append(escapeColumnNamesEnd);
        builder.append(" ");

        if (f.isPrimaryKey() && f.getType().equals(Schema.Type.STRING)) {
          builder.append("VARCHAR(50)");
        } else {
          builder.append(getSqlType(f.getType()));
        }

        if (f.isPrimaryKey()) {
          builder.append(" NOT NULL");
        } else {
          builder.append(" NULL");
        }
      }
    });

    final List<String> pks = new ArrayList<>();
    for (SinkRecordField f: fields) {
      if (f.isPrimaryKey()) {
        pks.add(f.getName());
      }
    }

    if (!pks.isEmpty()) {
      builder.append(",");
      builder.append(lineSeparator);
      builder.append("PRIMARY KEY(");
      joinToBuilder(builder, ",", pks, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
      builder.append(")");
    }

    builder.append(")");
    return builder.toString();
  }

  /**
   * Returns the query to alter a table by adding a new table
   *
   * @param tableName -The name of the table
   * @param fields - The list of table columns
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
    builder.append(" ");

    joinToBuilder(builder, ",", fields, new Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField f) {
        builder.append(lineSeparator);
        builder.append("ADD COLUMN ");
        builder.append(escapeColumnNamesStart);
        builder.append(f.getName());
        builder.append(escapeColumnNamesEnd);
        builder.append(" ");
        builder.append(getSqlType(f.getType()));
        builder.append(" NULL");
      }
    });

    return Collections.singletonList(builder.toString());
  }

  /**
   * Maps the Schema type to a database data type.
   *
   * @param type - The connect field type
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

  /**
   * Returns the escaped name of the table
   *
   * @param tableName - The table name
   * @return Table name
   */
  protected String handleTableName(String tableName) {
    return escapeColumnNamesStart + tableName + escapeColumnNamesEnd;
  }

  /**
   * Extracts the database protocol from a jdbc URI.
   *
   * @param connection- JDBC Connection instance
   * @return The sql protocol
   */
  static String extractProtocol(final String connection) {
    ParameterValidator.notNullOrEmpty(connection, "connection");
    if (!connection.startsWith("jdbc:")) {
      throw new IllegalArgumentException("connection is not a valid jdbc URI");
    }

    int index = connection.indexOf("://", "jdbc:".length());
    if (index < 0) {
      throw new IllegalArgumentException(String.format("%s is not a valid jdbc uri.", connection));
    }
    return connection.substring("jdbc:".length(), index);
  }
}