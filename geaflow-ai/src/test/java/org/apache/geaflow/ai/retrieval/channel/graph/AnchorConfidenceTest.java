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
 */
package org.apache.geaflow.ai.retrieval.channel.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.junit.jupiter.api.Test;

/**
 * Pins anchor confidence as one named constant per match type.
 *
 * <p>The values previously appeared twice: once while building the token index and once in the
 * linear scan used when the query equals a name exactly. Two copies of the same threshold drift
 * apart silently, and anchor confidence decides whether a query is allowed a second hop.
 */
class AnchorConfidenceTest {

    @Test
    void exposesOneNamedConfidencePerMatchType() {
        assertEquals(1.0D, EntityAnchorResolver.CANONICAL_CONFIDENCE);
        assertEquals(0.9D, EntityAnchorResolver.ALIAS_CONFIDENCE);
        assertEquals(0.35D, EntityAnchorResolver.TEXT_CANDIDATE_CONFIDENCE);
    }

    @Test
    void bothResolutionPathsReportTheSameConfidence() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", Arrays.asList("kongzi"), "person",
                Collections.singletonList("c1")));
        EntityAnchorResolver resolver = new EntityAnchorResolver(entities);

        // Whole-query equality takes the linear path; a token-containing query takes the index path.
        double exactCanonical = resolver.resolve("Confucius", entities).get(0).getConfidence();
        double indexedCanonical = resolver.resolve("Who was Confucius", entities).get(0).getConfidence();
        double exactAlias = resolver.resolve("kongzi", entities).get(0).getConfidence();
        double indexedAlias = resolver.resolve("tell me about kongzi", entities).get(0).getConfidence();

        assertEquals(EntityAnchorResolver.CANONICAL_CONFIDENCE, exactCanonical);
        assertEquals(EntityAnchorResolver.CANONICAL_CONFIDENCE, indexedCanonical);
        assertEquals(EntityAnchorResolver.ALIAS_CONFIDENCE, exactAlias);
        assertEquals(EntityAnchorResolver.ALIAS_CONFIDENCE, indexedAlias);
    }

    @Test
    void textCandidateFallbackUsesTheNamedConfidence() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", Collections.emptyList(), "person",
                Collections.singletonList("c1")));
        EntityAnchorResolver resolver = new EntityAnchorResolver(entities);

        List<EntityAnchorResolver.Anchor> anchors = resolver.resolveFromCandidates("unmatched",
            entities, Collections.singletonList("c1"), 5, 0L, System::nanoTime);

        assertEquals(EntityAnchorResolver.TEXT_CANDIDATE_CONFIDENCE, anchors.get(0).getConfidence());
        assertEquals("TEXT_CANDIDATE", anchors.get(0).getMatchType());
    }
}