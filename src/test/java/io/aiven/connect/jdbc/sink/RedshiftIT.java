/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
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
 */

package io.aiven.connect.jdbc.sink;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.streamkap.common.test.sink.StreamkapSinkITBase;

public class RedshiftIT extends StreamkapSinkITBase<JdbcSinkTask> {

    @Override
    protected JdbcSinkTask createSinkTask() {
        return new JdbcSinkTask();
    }

    @Override
    protected Map<String, String> getConf() {
        Map<String, String> config = new HashMap<>();

        config.put("connection.url", "jdbc:redshift://admin.424842667740.us-west-2.redshift-serverless.amazonaws.com:5439/dev?user=streamkap_user&password=Admin1234");
        config.put("connection.user", "streamkap_user");
        config.put("connection.password", "Admin1234");
        config.put("dialect.name", "RedshiftDatabaseDialect");
        config.put("insert.mode", "multi");
        config.put("auto.create", "true");

        return config;
    }
 
    protected String getSchemaName() {
        return "dev.public";
    }
 
    protected SinkRecord applyTransforms(SinkRecord record) {
        return record;
    }
 
    @Test
    public void testNominalInsert() throws SQLException, InterruptedException {
        super.testNominalInsert(true);
    }
}
