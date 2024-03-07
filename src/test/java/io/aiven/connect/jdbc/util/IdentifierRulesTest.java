/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2019 Confluent Inc.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IdentifierRulesTest {

    private IdentifierRules rules;

    @BeforeEach
    public void beforeEach() {
        rules = IdentifierRules.DEFAULT;
    }

    @Test
    public void testParsingWithMultiCharacterQuotes() {
        rules = new IdentifierRules(".", "'''", "'''");
        assertThat(rules.parseQualifiedIdentifier("'''p1'''.'''p2'''.'''p3'''")).containsExactly("p1", "p2", "p3");
        assertThat(rules.parseQualifiedIdentifier("'''p1'''.'''p3'''")).containsExactly("p1", "p3");
        assertThat(rules.parseQualifiedIdentifier("'''p1'''")).containsExactly("p1");
        assertThat(rules.parseQualifiedIdentifier("'''p1.1.2.3'''")).containsExactly("p1.1.2.3");
        assertThat(rules.parseQualifiedIdentifier("'''p1.1.2.3.'''")).containsExactly("p1.1.2.3.");
        assertThat(rules.parseQualifiedIdentifier("")).containsExactly("");
        assertParsingFailure("'''p1.p2"); // unmatched quote
        assertParsingFailure("'''p1'''.'''p3'''."); // ends with delim
    }

    @Test
    public void testParsingWithDifferentLeadingAndTrailingQuotes() {
        rules = new IdentifierRules(".", "[", "]");
        assertThat(rules.parseQualifiedIdentifier("[p1].[p2].[p3]")).containsExactly("p1", "p2", "p3");
        assertThat(rules.parseQualifiedIdentifier("[p1].[p3]")).containsExactly("p1", "p3");
        assertThat(rules.parseQualifiedIdentifier("[p1]")).containsExactly("p1");
        assertThat(rules.parseQualifiedIdentifier("[p1.1.2.3]")).containsExactly("p1.1.2.3");
        assertThat(rules.parseQualifiedIdentifier("[p1[.[1.[2.3]")).containsExactly("p1[.[1.[2.3");
        assertThat(rules.parseQualifiedIdentifier("")).containsExactly("");
        assertParsingFailure("[p1].[p3]."); // ends with delim
    }

    @Test
    public void testParsingWithSingleCharacterQuotes() {
        rules = new IdentifierRules(".", "'", "'");
        assertThat(rules.parseQualifiedIdentifier("'p1'.'p2'.'p3'")).containsExactly("p1", "p2", "p3");
        assertThat(rules.parseQualifiedIdentifier("'p1'.'p3'")).containsExactly("p1", "p3");
        assertThat(rules.parseQualifiedIdentifier("'p1'")).containsExactly("p1");
        assertThat(rules.parseQualifiedIdentifier("'p1.1.2.3'")).containsExactly("p1.1.2.3");
        assertThat(rules.parseQualifiedIdentifier("")).containsExactly("");
        assertParsingFailure("'p1'.'p3'."); // ends with delim
    }

    @Test
    public void testParsingWithoutQuotes() {
        rules = new IdentifierRules(".", "'", "'");
        assertThat(rules.parseQualifiedIdentifier("p1.p2.p3")).containsExactly("p1", "p2", "p3");
        assertThat(rules.parseQualifiedIdentifier("p1.p3")).containsExactly("p1", "p3");
        assertThat(rules.parseQualifiedIdentifier("p1")).containsExactly("p1");
        assertThat(rules.parseQualifiedIdentifier("")).containsExactly("");
        assertParsingFailure("'p1'.'p3'."); // ends with delim
        assertParsingFailure("p1.p3."); // ends with delim
    }

    @Test
    public void testParsingWithUnsupportedQuotes() {
        rules = new IdentifierRules(".", " ", " ");
        assertThat(rules.parseQualifiedIdentifier("p1.p2.p3")).containsExactly("p1", "p2", "p3");
        assertThat(rules.parseQualifiedIdentifier("p1.p3")).containsExactly("p1", "p3");
        assertThat(rules.parseQualifiedIdentifier("p1")).containsExactly("p1");
        assertThat(rules.parseQualifiedIdentifier("")).containsExactly("");
    }

    protected void assertParsingFailure(final String fqn) {
        assertThatThrownBy(() -> rules.parseQualifiedIdentifier(fqn))
            .as("expected parsing error")
            .isInstanceOf(IllegalArgumentException.class);
    }
}
