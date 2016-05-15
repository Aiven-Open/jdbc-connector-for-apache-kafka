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

//package com.datamountaineer.streamreactor.connect.jdbc.sink;
//
//
//import com.datamountaineer.streamreactor.connect.jdbc.sink.config.*;
//import com.fasterxml.jackson.databind.*;
//import com.google.common.collect.*;
//import org.apache.kafka.common.config.*;
//
//import java.io.*;
//import java.util.*;
//
//class Main
//{
//  public static void main(String[] args)
//  {
//
//    String raw="[{topic1:table1;f1->col1,f2->col2,f3->col5}, {topic2:table2;f5->col5,f6->col7}]";
//    String[] rawMappings = raw.split("\\},");
//
//    for (String rawMapping: rawMappings) {
//      String[] split = rawMapping.replace("[{", "").replace("}", "").replace("}]", "").trim().split(";");
//
//      if (split.length != 2) {
//        throw new ConfigException(String.format(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set correctly, %s", rawMapping));
//      }
//
//      String[] tTot = split[0].split(":");
//      String[] fToCol = split[1].split(",");
//
//      if (tTot.length != 2) {
//        throw new ConfigException(String.format(JdbcSinkConfig.EXPORT_MAPPINGS + " is not set correctly, %s. Missing " +
//            "either the table or topic.", rawMapping));
//      }
//
//      String topic = tTot[0];
//      String table = tTot[1];
//
//      Map<String, FieldAlias> mappings = Maps.newHashMap();
//
//      for (String mapping : fToCol) {
//        String[] colSplit = mapping.split("->");
//        String fieldName = colSplit[0];
//        String colName;
//
//        if (colSplit.length == 2) {
//          colName = colSplit[1];
//        } else {
//          colName = colSplit[0];
//        }
//
//        mappings.put(fieldName, new FieldAlias(colName, false));
//      }
//
//      FieldsMappings fieldMappings = new FieldsMappings(table, topic, true, mappings);
//    }
//
//
//    ObjectMapper mapper = new ObjectMapper();
//    String jsonInString = "[{\n" +
//        "\t\"topic\": \"topic1\",\n" +
//        "\t\"table\": \"table1\",\n" +
//        "\t\"fieldMappings\": [{\n" +
//        "\t\t\"kafkaField\": \"f1\",\n" +
//        "\t\t\"columnName\": \"col1\",\n" +
//        "\t\t\"pk\": \"true\"\n" +
//        "\t}, {\n" +
//        "\t\t\"kafkaField\": \"f2\",\n" +
//        "\t\t\"columnName\": \"col2\",\n" +
//        "\t\t\"pk\": \"false\"\n" +
//        "\t}]\n" +
//        "}]";
//
//
//
//    try {
//      FieldsMappings obj = mapper.readValue(jsonInString, FieldsMappings.class);
//      obj.getTableName();
//    } catch (IOException e1) {
//      e1.printStackTrace();
//    }
//  }
//
//}