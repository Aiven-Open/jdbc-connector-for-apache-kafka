/**
 * Copyright 2015 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JDBCUrl;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by andrew@datamountaineer.com on 10/05/16.
 * kafka-connect-jdbc
 */
public class TableMonitor {
  private static final Logger log = LoggerFactory.getLogger(TableMonitor.class);

  private static final String COLUMN_NAME = "COLUMN_NAME";
  private static final String COLUMN_TYPE = "DATA_TYPE";
  private static final String TABLE_NAME = "TABLE_NAME";
  private Connection db = null;
  private static String database;
  private HashMap<String, HashMap<String, String>> metadataCached = new HashMap<>();


  public TableMonitor(JdbcSinkSettings settings) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(settings.getConnection());

    try {
      this.db = new HikariDataSource(config).getConnection();
    } catch (SQLException e) {
      log.error(String.format("Unable to connect to database %s.", database), e);
    }

    String databaseName = JDBCUrl.getDatabaseName(settings.getConnection());
    this.database = databaseName;

    HashMap<String, HashMap<String, String>> map = new HashMap<>();
    //get columns
    for (FieldsMappings mappings : settings.getMappings()) {
      String tableName = mappings.getTableName();
      HashMap<String, String> columns = buildTableMap(tableName);
      map.put(tableName, columns);
    }
    this.metadataCached = map;
  }

  public  HashMap<String, HashMap<String, String>> getCache() {
    return metadataCached;
  }

  /**
   * Build the tables, column meta map.
   *
   * @return a Map of tables columns and column type for the configured tables.
   * */
  public  HashMap<String, String> buildTableMap(String table) {
    HashMap<String, String> colMap = new HashMap<>();

    try {
      DatabaseMetaData meta = db.getMetaData();
      String   catalog          = database;
      String   schemaPattern    = null;
      String   tableNamePattern = null;
      String[] types            = null;
      ResultSet tablesRS = meta.getTables(catalog, schemaPattern, tableNamePattern, types);

      //collect the columns for our tables
      while (tablesRS.next()) {
        String tableRs = tablesRS.getString(TABLE_NAME);

        //filter for our tables
        if (tableRs.equals(table)) {
          ResultSet colsRs = meta.getColumns(catalog, null, table, null);

          colMap.clear();
          while (colsRs.next()) {
            String colName = colsRs.getString(COLUMN_NAME);
            String colType = colsRs.getString(COLUMN_TYPE);
            colMap.put(colName, colType);
          }
          colsRs.close();
        }
      }
    } catch (SQLException e) {
      throw new ConnectException(String.format("Error retrieving meta data for database %s.", database), e);
    }
    return colMap;
  }

  /**
   * Check if the a reconfiguration is required for this table.
   *
   * @param tableName The table name to check the database for DDL changes.
   * @param allFields If the writer to reconfigure is configured to select all fields.
   * @return A boolean flag indicating if the table has changed.
   * */
  public Boolean doReconfigure(String tableName, Boolean allFields) {
    HashMap<String, String> map = buildTableMap(tableName);
    if (map.size() == 0) {
      log.warn(String.format("Removing table %s from TableMonitor Cache.", tableName));
      metadataCached.remove(tableName);
      return true;
    } else {
      return checkColumns(map, tableName, allFields);
    }
  }


  /**
   * Check if we need to reconfigure the task because of a
   * change in the database tables
   *
   * @return  a Boolean indicating if a change should occur.
   * */
  public Boolean checkColumns(HashMap<String, String> current, String tableName, Boolean allFields) {
    HashMap<String, String> cachedColumns = new HashMap<>();

    //get cached previous set for this table
    if (metadataCached.containsKey(tableName)) {
      cachedColumns = metadataCached.get(tableName);
    }

    /* If we are all fields simply check number of columns, if additions or removals trigger reconfigure
     * The writer will rebind.
     *
     * if we have explicit column mappings check those columns still exist
     */
    if (allFields && cachedColumns.size() != current.size()) {
      log.warn(String.format("Difference in number of columns detected for table %s", tableName));
      metadataCached.put(tableName, current);
      return true;
    } else {
      //change is columns
      for (String col : cachedColumns.keySet()) {
        //does our column still exist
        if (current.containsKey(col)) {
          //check type
          String colType = cachedColumns.get(col);
          String newColType = current.get(col);
          if (!colType.equals(newColType)) {
            //difference in type check compatibility.
            log.warn(String.format("Difference in column types detected for column %s in tables %s.%s. New: %s. Old %s",
                col, database, tableName, newColType, colType));
            log.warn(String.format("Updating TableMonitor cached for table %s", tableName));
            metadataCached.put(tableName, current);
            return true;
          }

        } else {
          log.warn(String.format("Column %s no longer exists in tables %s.%s", col, database, tableName));
          log.warn(String.format("Updating TableMonitor cached for table %s", tableName));
          metadataCached.put(tableName, current);
          return true;
        }
      }
      //if here no change
      return false;
    }
  }

  public void closeDbConn() {

    try {
      db.close();
    } catch (SQLException e) {
      log.error(String.format("Error closing connection to database %s", database), e);
    }
  }
}
