/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2015 Confluent Inc.
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

package io.aiven.connect.jdbc.util;

import java.util.Objects;

public class TableId implements Comparable<TableId>, ExpressionBuilder.Expressable {

    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final int hash;

    public TableId(
        final String catalogName,
        final String schemaName,
        final String tableName
    ) {
        this.catalogName = catalogName == null || catalogName.isEmpty() ? null : catalogName;
        this.schemaName = schemaName == null || schemaName.isEmpty() ? null : schemaName;
        this.tableName = tableName;
        this.hash = Objects.hash(catalogName, schemaName, tableName);
    }

    public String catalogName() {
        return catalogName;
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    public TableId unqualified() {
        return new TableId(null, null, this.tableName);
    }

    @Override
    public void appendTo(final ExpressionBuilder builder, final boolean useQuotes) {
        if (catalogName != null) {
            builder.appendIdentifier(catalogName, useQuotes);
            builder.appendIdentifierDelimiter();
        }
        if (schemaName != null) {
            builder.appendIdentifier(schemaName, useQuotes);
            builder.appendIdentifierDelimiter();
        }
        builder.appendIdentifier(tableName, useQuotes);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TableId) {
            final TableId that = (TableId) obj;
            return Objects.equals(this.catalogName, that.catalogName)
                && Objects.equals(this.schemaName, that.schemaName)
                && Objects.equals(this.tableName, that.tableName);
        }
        return false;
    }

    @Override
    public int compareTo(final TableId that) {
        if (that == this) {
            return 0;
        }
        int diff = this.tableName.compareTo(that.tableName);
        if (diff != 0) {
            return diff;
        }
        if (this.schemaName == null) {
            if (that.schemaName != null) {
                return -1;
            }
        } else {
            if (that.schemaName == null) {
                return 1;
            }
            diff = this.schemaName.compareTo(that.schemaName);
            if (diff != 0) {
                return diff;
            }
        }
        if (this.catalogName == null) {
            if (that.catalogName != null) {
                return -1;
            }
        } else {
            if (that.catalogName == null) {
                return 1;
            }
            diff = this.catalogName.compareTo(that.catalogName);
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return ExpressionBuilder.create().append(this).toString();
    }
}
