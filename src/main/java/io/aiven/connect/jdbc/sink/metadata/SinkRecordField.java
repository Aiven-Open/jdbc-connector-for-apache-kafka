/*
 * Copyright 2021 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

package io.aiven.connect.jdbc.sink.metadata;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;

public class SinkRecordField {

    private final Schema schema;
    private final String name;
    private final boolean isPrimaryKey;

    public SinkRecordField(final Schema schema, final String name, final boolean isPrimaryKey) {
        this.schema = schema;
        this.name = name;
        this.isPrimaryKey = isPrimaryKey;
    }

    public Schema schema() {
        return schema;
    }

    public String schemaName() {
        return schema.name();
    }

    public Map<String, String> schemaParameters() {
        return schema.parameters();
    }

    public Schema.Type schemaType() {
        return schema.type();
    }

    public String name() {
        return name;
    }

    public boolean isOptional() {
        return !isPrimaryKey && schema.isOptional();
    }

    public Object defaultValue() {
        return schema.defaultValue();
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    @Override
    public String toString() {
        return "SinkRecordField{"
            + "schema=" + schema
            + ", name='" + name + '\''
            + ", isPrimaryKey=" + isPrimaryKey
            + '}';
    }
}
