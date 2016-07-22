package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;

public class BufferedRecords {
  private static final Logger logger = LoggerFactory.getLogger(BufferedRecords.class);

  private final String tableName;
  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new LinkedList<>();
  private SchemaPair currentSchemaPair;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement preparedStatement;
  private PreparedStatementBinder preparedStatementBinder;

  public BufferedRecords(JdbcSinkConfig config, String tableName, DbDialect dbDialect, DbStructure dbStructure, Connection connection) {
    this.tableName = tableName;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

    if (currentSchemaPair == null) {
      currentSchemaPair = schemaPair;
      // re-initialize everything that depends on the record schema
      fieldsMetadata = FieldsMetadata.extract(tableName, config.pkMode, config.pkFields, currentSchemaPair);
      dbStructure.createOrAmendIfNecessary(config, connection, tableName, fieldsMetadata);
      final String insertSql = getInsertSql();
      logger.debug("insertion sql:{}", config.insertMode, insertSql);
      preparedStatement = connection.prepareStatement(insertSql);
      preparedStatementBinder = new PreparedStatementBinder(preparedStatement, config.pkMode, schemaPair, fieldsMetadata);
    }

    final List<SinkRecord> flushed;
    if (currentSchemaPair.equals(schemaPair)) {
      // Continue with current batch state
      records.add(record);
      if (records.size() >= config.batchSize) {
        flushed = flush();
      } else {
        flushed = Collections.emptyList();
      }
    } else {
      // Each batch needs to have the same SchemaPair, so get the buffered records out, reset state and re-attempt the add
      flushed = flush();
      currentSchemaPair = null;
      flushed.addAll(add(record));
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      return new LinkedList<>();
    }
    for (SinkRecord record : records) {
      preparedStatementBinder.bindRecord(record);
    }
    int totalUpdateCount = 0;
    for (int updateCount : preparedStatement.executeBatch()) {
      totalUpdateCount += updateCount;
    }
    if (totalUpdateCount != records.size()) {
      switch (config.insertMode) {
        case INSERT:
          throw new ConnectException(String.format("Update count (%d) did not sum up to total number of records inserted (%d)",
                                                   totalUpdateCount, records.size()));
        case UPSERT:
          logger.debug("Upserted records:{} resulting in in totalUpdateCount:{}", records.size(), totalUpdateCount);
      }
    }
    connection.commit();

    final List<SinkRecord> flushedRecords = records;
    records = new LinkedList<>();
    return flushedRecords;
  }

  private String getInsertSql() {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.getInsert(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the primary key configuration", tableName
          ));
        }
        return dbDialect.getUpsertQuery(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }
}
