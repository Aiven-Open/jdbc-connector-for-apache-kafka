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

package io.aiven.connect.jdbc.sink.metadata;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;

public class SchemaPair {
    public final Schema keySchema;
    public final Schema valueSchema;

    public SchemaPair(final Schema keySchema, final Schema valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SchemaPair that = (SchemaPair) o;
        return Objects.equals(keySchema, that.keySchema)
            && Objects.equals(valueSchema, that.valueSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySchema, valueSchema);
    }

    public String toString() {
        return String.format("<SchemaPair: %s, %s>", keySchema, valueSchema);
    }
}
