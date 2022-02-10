/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

public class ColumnId implements ExpressionBuilder.Expressable {

    private final TableId tableId;
    private final String name;
    private final String alias;
    private final int hash;

    public ColumnId(
        final TableId tableId,
        final String columnName
    ) {
        this(tableId, columnName, null);
    }

    public ColumnId(
        final TableId tableId,
        final String columnName,
        final String alias
    ) {
        assert columnName != null;
        this.tableId = tableId;
        this.name = columnName;
        this.alias = alias != null && !alias.trim().isEmpty() ? alias : name;
        this.hash = Objects.hash(this.tableId, this.name);
    }

    public TableId tableId() {
        return tableId;
    }

    public String name() {
        return name;
    }

    /**
     * Gets the column's suggested title for use in printouts and displays. The suggested title is
     * usually specified by the SQL <code>AS</code> clause.  If a SQL <code>AS</code> is not
     * specified, the value will be the same as the value returned by the {@link #name()} method.
     *
     * @return the suggested column title; never null
     */
    public String aliasOrName() {
        return alias;
    }

    @Override
    public void appendTo(final ExpressionBuilder builder, final boolean useQuotes) {
        if (tableId != null) {
            builder.append(tableId);
            builder.appendIdentifierDelimiter();
        }
        builder.appendIdentifier(this.name, useQuotes);
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
        if (obj instanceof ColumnId) {
            final ColumnId that = (ColumnId) obj;
            return Objects.equals(this.name, that.name) && Objects.equals(this.alias, that.alias)
                && Objects.equals(this.tableId, that.tableId);
        }
        return false;
    }

    @Override
    public String toString() {
        return ExpressionBuilder.create().append(this).toString();
    }
}
