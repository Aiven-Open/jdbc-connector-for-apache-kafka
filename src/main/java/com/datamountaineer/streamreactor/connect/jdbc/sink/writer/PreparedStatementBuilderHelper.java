package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTableColumn;
import com.datamountaineer.streamreactor.connect.jdbc.sink.RecordDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.InsertModeEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import io.confluent.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class PreparedStatementBuilderHelper {

  private static final Logger logger = LoggerFactory.getLogger(PreparedStatementBuilderHelper.class);

  /**
   * This is used in the non evolving table mode. It will overlap the database columns information over the one provided
   * by the user - provides validation and also allows for columns auto mapping (Say user flags all fields to be included
   * and the payload has only 3 ouf of the 4 incoming mapping. It will therefore only consider the 3 fields discarding the
   * 4th avoiding the error)
   *
   * @param settings
   * @param databaseMetadata
   * @return
   */
  public static PreparedStatementContextIterable from(final JdbcSinkSettings settings,
                                                      final DatabaseMetadata databaseMetadata) {

    final Map<String, DataExtractorWithQueryBuilder> map = Maps.newHashMap();
    for (final FieldsMappings tm : settings.getMappings()) {
      FieldsMappings tableMappings = tm;
      //if the table is not set with autocreate we try to find it
      if (!tm.autoCreateTable()) {
        if (!databaseMetadata.containsTable(tm.getTableName())) {
          final String tables = Joiner.on(",").join(databaseMetadata.getTableNames());
          throw new ConfigException(String.format("%s table is not found in the database available tables:%s. Make sure you" +
                          " set the table to be autocreated or manually add it to the database.",
                  tm.getTableName(),
                  tables));
        }
        //get the columns merged
        tableMappings = validateAndMerge(tm, databaseMetadata.getTable(tm.getTableName()));
      }
      final RecordDataExtractor fieldsValuesExtractor = new RecordDataExtractor(tableMappings);
      final QueryBuilder queryBuilder = QueryBuilderHelper.from(settings.getConnection(), tm.getInsertMode());

      map.put(tm.getIncomingTopic().toLowerCase(), new DataExtractorWithQueryBuilder(queryBuilder, fieldsValuesExtractor));

    }

    return new PreparedStatementContextIterable(map, settings.getBatchSize());
  }

  /**
   * If the table is not set to autoevelovewe merge the field mappings with the database structure.
   * This way we avoid the problems caused when the user sets * for all fields the payload has 4 fields but only 3 found in the database.
   * Furthermore if the mapping is not found in the database an exception is thrown
   *
   * @param tm      - Field mappings
   * @param dbTable - The instance of DbTable
   * @return
   */
  public static FieldsMappings validateAndMerge(final FieldsMappings tm, final DbTable dbTable) {
    final Set<String> pkColumns = new HashSet<>();
    final Map<String, DbTableColumn> dbCols = dbTable.getColumns();
    for (DbTableColumn column : dbCols.values()) {
      if (column.isPrimaryKey()) {
        pkColumns.add(column.getName());
      }
    }
    final Map<String, FieldAlias> map = new HashMap<>();
    if (tm.areAllFieldsIncluded()) {

      for (DbTableColumn column : dbTable.getColumns().values()) {
        map.put(column.getName(),
                new FieldAlias(column.getName(), column.isPrimaryKey()));
      }

      //apply the specific mappings
      for (Map.Entry<String, FieldAlias> kv : tm.getMappings().entrySet()) {
        final String colName = kv.getValue().getName();
        if (!map.containsKey(colName.toLowerCase()) && !map.containsKey(colName.toUpperCase())) {
          final String error =
                  String.format("Invalid field mapping. For table %s the following column is not found %s in available columns:%s",
                          tm.getTableName(),
                          colName,
                          Joiner.on(",").join(map.keySet()));
          throw new ConfigException(error);
        }

        //we want to add this entry. kv.getKey is the payload field which gets mapped on the column
        map.put(kv.getKey(), new FieldAlias(colName, pkColumns.contains(colName.toLowerCase()) || pkColumns.contains(colName.toUpperCase())));
      }
    } else {

      final Set<String> specifiedPKs = new HashSet<>();
      //in this case just validate the mappings
      for (Map.Entry<String, FieldAlias> alias : tm.getMappings().entrySet()) {
        final String colName = alias.getValue().getName();
        if (!dbCols.containsKey(colName.toLowerCase()) && !dbCols.containsKey(colName.toLowerCase())) {
          final String error =
                  String.format("Invalid field mapping. For table %s the following column is not found %s in available columns:%s",
                          tm.getTableName(),
                          colName,
                          Joiner.on(",").join(dbCols.keySet()));
          throw new ConfigException(error);
        }
        map.put(alias.getKey(), new FieldAlias(colName, pkColumns.contains(colName.toLowerCase()) || pkColumns.contains(colName.toUpperCase())));
        if (pkColumns.contains(colName.toLowerCase()) || pkColumns.contains(colName.toUpperCase())) {
          specifiedPKs.add(colName);
        }
      }

      if (pkColumns.size() > 0) {
        if (!specifiedPKs.containsAll(pkColumns)) {
          logger.warn("Invalid mappings. Not all PK columns have been specified. PK specified {} out of existing {}",
                  Joiner.on(",").join(specifiedPKs),
                  Joiner.on(",").join(pkColumns));
        }
        if (tm.getInsertMode().equals(InsertModeEnum.UPSERT)) {
          throw new ConfigException(
                  String.format("Invalid mappings. Not all PK columns have been specified. PK specified %s  out of existing %s",
                          Joiner.on(",").join(specifiedPKs),
                          Joiner.on(",").join(pkColumns)));
        }
      }
    }
    return new FieldsMappings(tm.getTableName(),
            tm.getIncomingTopic(),
            tm.areAllFieldsIncluded(),
            tm.getInsertMode(),
            map,
            tm.autoCreateTable(),
            tm.evolveTableSchema(),
            tm.isCapitalizeColumnNamesAndTables());
  }
}