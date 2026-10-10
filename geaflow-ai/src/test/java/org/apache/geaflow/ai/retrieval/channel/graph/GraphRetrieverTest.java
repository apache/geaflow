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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.channel.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.junit.jupiter.api.Test;

class GraphRetrieverTest {

    @Test
    void edgeCapStopsNeighborIterationWithoutCopyingOrSortingTheWholeList() {
        AtomicInteger reads = new AtomicInteger();
        List<GraphEdgeRef> neighbors = new AbstractList<GraphEdgeRef>() {
            @Override
            public GraphEdgeRef get(int index) {
                reads.incrementAndGet();
                if (index >= 2) {
                    throw new AssertionError("retriever read beyond edge cap");
                }
                return edge(index);
            }

            @Override
            public int size() {
                return 100_000;
            }
        };
        GraphNeighborProvider provider = entityId -> neighbors;
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "Confucius"));
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities, provider,
            chunks, 10, 2, 0L, System::nanoTime);

        assertEquals(2, hits.size());
        assertEquals(2, reads.get());
        assertEquals(2, retriever.getLastStats().getEdgesExamined());
    }

    @Test
    void expiredDeadlineDoesNotReadNeighbors() {
        AtomicInteger reads = new AtomicInteger();
        GraphNeighborProvider provider = entityId -> new AbstractList<GraphEdgeRef>() {
            @Override
            public GraphEdgeRef get(int index) {
                reads.incrementAndGet();
                return edge(index);
            }

            @Override
            public int size() {
                return 100_000;
            }
        };
        GraphRetriever retriever = new GraphRetriever();
        List<EntityRef> entities = Collections.singletonList(new EntityRef("e1", "Confucius", "person"));

        retriever.retrieve("Confucius", entities, provider, Collections.emptyMap(),
            10, 10, 1L, () -> 1L);

        assertEquals(0, reads.get());
    }

    @Test
    void duplicateProvenanceScanStopsAtCandidateBudget() {
        List<String> provenance = new java.util.ArrayList<>();
        for (int index = 0; index < 1000; index++) {
            provenance.add("c1");
        }
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "supports", "e1", "e2", provenance);
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "Confucius"));
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            Collections.singletonList(edge), chunks, 4, 10, 0L);

        assertEquals(1, hits.size());
        assertEquals(1, retriever.getLastStats().getCandidatesProduced());
        assertEquals(RecallStopReason.CANDIDATE_LIMIT, retriever.getLastStats().getStopReason());
    }

    private static GraphEdgeRef edge(int index) {
        return new GraphEdgeRef(String.format("edge-%06d", index), "supports", "e1", "e2",
            Collections.singletonList("c1"));
    }
}
