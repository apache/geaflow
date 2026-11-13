/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.temporal.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoryFactTest {

    @Test
    public void testAttributeFact() {
        MemoryEntity alice =
            new MemoryEntity("person:alice", "person");
        MemoryFact fact = MemoryFact.attribute(
            "fact-name-alice",
            alice,
            "name",
            "Alice");
        MemoryFact same = MemoryFact.attribute(
            "fact-name-alice",
            new MemoryEntity("person:alice", "person"),
            "name",
            "Alice");

        Assertions.assertEquals("fact-name-alice", fact.getId());
        Assertions.assertEquals(alice, fact.getSubject());
        Assertions.assertEquals("name", fact.getPredicate());
        Assertions.assertFalse(fact.isRelationship());
        Assertions.assertEquals(
            "Alice",
            fact.getLiteralValue().get());
        Assertions.assertFalse(fact.getTarget().isPresent());
        Assertions.assertEquals(fact, same);
        Assertions.assertEquals(fact.hashCode(), same.hashCode());

        Assertions.assertNotEquals(
            fact,
            MemoryFact.attribute(
                "fact-name-alice",
                alice,
                "name",
                "Alice Smith"));
    }

    @Test
    public void testRelationshipFact() {
        MemoryEntity alice =
            new MemoryEntity("person:alice", "person");
        MemoryEntity acme =
            new MemoryEntity("company:acme", "company");
        MemoryFact fact = MemoryFact.relationship(
            "fact-alice-acme",
            alice,
            "worksAt",
            acme);
        MemoryFact same = MemoryFact.relationship(
            "fact-alice-acme",
            new MemoryEntity("person:alice", "person"),
            "worksAt",
            new MemoryEntity("company:acme", "company"));

        Assertions.assertTrue(fact.isRelationship());
        Assertions.assertFalse(fact.getLiteralValue().isPresent());
        Assertions.assertEquals(acme, fact.getTarget().get());
        Assertions.assertEquals(fact, same);
        Assertions.assertEquals(fact.hashCode(), same.hashCode());

        Assertions.assertNotEquals(
            fact,
            MemoryFact.relationship(
                "fact-alice-acme",
                alice,
                "worksAt",
                new MemoryEntity("company:other", "company")));
    }

    @Test
    public void testRejectInvalidFact() {
        MemoryEntity alice =
            new MemoryEntity("person:alice", "person");

        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryFact.attribute(null, alice, "name", "Alice"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MemoryFact.attribute(" ", alice, "name", "Alice"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryFact.attribute(
                "fact-name-alice",
                null,
                "name",
                "Alice"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MemoryFact.attribute(
                "fact-name-alice",
                alice,
                " ",
                "Alice"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryFact.attribute(
                "fact-name-alice",
                alice,
                "name",
                null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MemoryFact.attribute(
                "fact-name-alice",
                alice,
                "name",
                " "));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryFact.relationship(
                "fact-alice-acme",
                alice,
                "worksAt",
                null));
    }
}
