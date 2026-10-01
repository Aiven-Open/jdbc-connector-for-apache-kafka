/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionBuilderTest {

    @Test
    public void testAppendIdentifierWithEmbeddedQuote() {
        // A field name containing a double-quote should be escaped by doubling
        final ExpressionBuilder builder = ExpressionBuilder.create();
        builder.appendIdentifier("col\" OR 1=1 --");
        // The embedded " must be escaped to "" per SQL standard
        assertThat(builder.toString()).isEqualTo("\"col\"\" OR 1=1 --\"");
    }

    @Test
    public void testAppendIdentifierWithEmbeddedQuoteAndBoolean() {
        final ExpressionBuilder builder = ExpressionBuilder.create();
        builder.appendIdentifier("col\" OR 1=1 --", true);
        assertThat(builder.toString()).isEqualTo("\"col\"\" OR 1=1 --\"");
    }

    @Test
    public void testAppendIdentifierWithoutQuote() {
        final ExpressionBuilder builder = ExpressionBuilder.create();
        builder.appendIdentifier("normal_col", false);
        assertThat(builder.toString()).isEqualTo("normal_col");
    }

    @Test
    public void testAppendIdentifierNoEmbeddedQuote() {
        final ExpressionBuilder builder = ExpressionBuilder.create();
        builder.appendIdentifier("safe_column");
        assertThat(builder.toString()).isEqualTo("\"safe_column\"");
    }

    @Test
    public void testAppendIdentifierWithMySQLBacktick() {
        final IdentifierRules mysqlRules = new IdentifierRules("`");
        final ExpressionBuilder builder = new ExpressionBuilder(mysqlRules, true);
        builder.appendIdentifier("col` OR 1=1 --");
        assertThat(builder.toString()).isEqualTo("`col`` OR 1=1 --`");
    }

    @Test
    public void testAppendIdentifierSQLInjectionDELETE() {
        // Simulates the DELETE WHERE clause injection scenario
        // Field name: id" OR 1=1 --
        // Without fix: DELETE FROM "t" WHERE "id" OR 1=1 --" = ?
        // With fix:    DELETE FROM "t" WHERE "id"" OR 1=1 --" = ?
        final ExpressionBuilder builder = ExpressionBuilder.create();
        builder.append("DELETE FROM \"t\" WHERE ");
        builder.appendIdentifier("id\" OR 1=1 --");
        builder.append(" = ?");
        assertThat(builder.toString()).isEqualTo("DELETE FROM \"t\" WHERE \"id\"\" OR 1=1 --\" = ?");
    }

    @Test
    public void testAppendIdentifierSQLInjectionINSERT() {
        // Simulates the INSERT column list injection scenario
        final ExpressionBuilder builder = ExpressionBuilder.create();
        builder.append("INSERT INTO \"t\" (");
        builder.appendIdentifier("name\", \"email\"); DROP TABLE users; --");
        builder.append(") VALUES (?)");
        assertThat(builder.toString()).isEqualTo(
            "INSERT INTO \"t\" (\"name\"\", \"email\"\"); DROP TABLE users; --\") VALUES (?)");
    }
}
