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

package org.apache.geaflow.ai.retrieval.channel.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.junit.jupiter.api.Test;

class EntityAnchorResolverTest {
    private final EntityAnchorResolver resolver = new EntityAnchorResolver();

    @Test
    void resolvesCanonicalNamesAndAliasesWithStableConfidenceOrdering() {
        EntityRef canonicalZ = new EntityRef("z", "Confucius", "person");
        EntityRef canonicalA = new EntityRef("a", "Ｃｏｎｆｕｃｉｕｓ", "person");
        EntityRef alias = new EntityRef("alias", "Master Kong", Collections.singletonList("kongzi"),
            "person", Collections.emptyList());

        List<EntityAnchorResolver.Anchor> anchors = resolver.resolve("  CONFUCIUS  ",
            Arrays.asList(canonicalZ, alias, canonicalA));
        assertEquals(Arrays.asList("a", "z"), entityIds(anchors));
        assertEquals("CANONICAL", anchors.get(0).getMatchType());
        assertEquals(1.0, anchors.get(0).getConfidence());

        anchors = resolver.resolve(" KONGZI ", Collections.singletonList(alias));
        assertEquals(Collections.singletonList("alias"), entityIds(anchors));
        assertEquals("ALIAS", anchors.get(0).getMatchType());
        assertEquals(0.9, anchors.get(0).getConfidence());
    }

    @Test
    void rejectsPartialAndUnknownMatchesAndReturnsNoAnchorsAfterDeadline() {
        EntityRef entity = new EntityRef("e1", "Confucius", Collections.singletonList("kongzi"),
            "person", Collections.emptyList());
        assertTrue(resolver.resolve("Confucius and astronomy", Collections.singletonList(entity)).isEmpty());
        assertTrue(resolver.resolve("unknown", Collections.singletonList(entity)).isEmpty());
        assertTrue(resolver.resolve("Confucius", Collections.singletonList(entity), 1L, () -> 1L).isEmpty());
    }

    private static List<String> entityIds(List<EntityAnchorResolver.Anchor> anchors) {
        java.util.ArrayList<String> ids = new java.util.ArrayList<>();
        for (EntityAnchorResolver.Anchor anchor : anchors) {
            ids.add(anchor.getEntity().getEntityId());
        }
        return ids;
    }
}
