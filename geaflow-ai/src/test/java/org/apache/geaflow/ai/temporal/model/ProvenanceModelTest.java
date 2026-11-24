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

public class ProvenanceModelTest {

    @Test
    public void testSourceValueSemantics() {
        Source source = new Source("source-1", "customer-database");
        Source same = new Source("source-1", "customer-database");

        Assertions.assertEquals("source-1", source.getId());
        Assertions.assertEquals("customer-database", source.getName());
        Assertions.assertEquals(source, same);
        Assertions.assertEquals(source.hashCode(), same.hashCode());
        Assertions.assertNotEquals(
            source,
            new Source("source-1", "archive-database"));
    }

    @Test
    public void testEvidenceValueSemantics() {
        Source source = new Source("source-1", "customer-database");
        Evidence evidence = new Evidence(
            "evidence-1",
            source,
            "Alice works at Acme");
        Evidence same = new Evidence(
            "evidence-1",
            new Source("source-1", "customer-database"),
            "Alice works at Acme");

        Assertions.assertEquals("evidence-1", evidence.getId());
        Assertions.assertEquals(source, evidence.getSource());
        Assertions.assertEquals(
            "Alice works at Acme",
            evidence.getContent());
        Assertions.assertEquals(evidence, same);
        Assertions.assertEquals(evidence.hashCode(), same.hashCode());
        Assertions.assertNotEquals(
            evidence,
            new Evidence("evidence-1", source, "Alice left Acme"));
    }

    @Test
    public void testRejectInvalidProvenance() {
        Source source = new Source("source-1", "customer-database");

        Assertions.assertThrows(
            NullPointerException.class,
            () -> new Source(null, "customer-database"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new Source(" ", "customer-database"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new Source("source-1", " "));

        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new Evidence(" ", source, "Alice works at Acme"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new Evidence("evidence-1", null, "Alice works at Acme"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new Evidence("evidence-1", source, " "));
    }
}
