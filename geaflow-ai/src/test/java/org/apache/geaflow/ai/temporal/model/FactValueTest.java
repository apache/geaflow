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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FactValueTest {

    @Test
    public void testLiteralAndEntityReference() {
        FactValue literal = FactValue.literal("Beijing");
        FactValue entityReference =
            FactValue.entityReference("city:beijing");

        Assertions.assertEquals(
            FactValue.Kind.LITERAL,
            literal.getKind());
        Assertions.assertEquals("Beijing", literal.getValue());
        Assertions.assertEquals(
            Optional.of("Beijing"),
            literal.getLiteralValue());
        Assertions.assertEquals(
            Optional.empty(),
            literal.getEntityId());

        Assertions.assertEquals(
            FactValue.Kind.ENTITY_REF,
            entityReference.getKind());
        Assertions.assertEquals(
            "city:beijing",
            entityReference.getValue());
        Assertions.assertEquals(
            Optional.empty(),
            entityReference.getLiteralValue());
        Assertions.assertEquals(
            Optional.of("city:beijing"),
            entityReference.getEntityId());
    }

    @Test
    public void testValueSemanticsAndDeterministicOrder() {
        FactValue beijing = FactValue.literal("Beijing");
        FactValue same = FactValue.literal("Beijing");
        FactValue shanghai = FactValue.literal("Shanghai");
        FactValue reference =
            FactValue.entityReference("city:beijing");

        Assertions.assertEquals(beijing, same);
        Assertions.assertEquals(beijing.hashCode(), same.hashCode());
        Assertions.assertNotEquals(beijing, reference);

        List<FactValue> values = new ArrayList<>(Arrays.asList(
            reference,
            shanghai,
            beijing));
        Collections.sort(values);

        Assertions.assertEquals(
            Arrays.asList(beijing, shanghai, reference),
            values);
    }

    @Test
    public void testRejectInvalidValue() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> FactValue.literal(null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> FactValue.literal(" "));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> FactValue.entityReference(null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> FactValue.entityReference(" "));
    }
}
