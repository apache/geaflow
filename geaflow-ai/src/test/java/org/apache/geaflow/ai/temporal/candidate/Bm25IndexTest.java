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

package org.apache.geaflow.ai.temporal.candidate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.Source;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Bm25IndexTest {

    @Test
    public void testUnicodeWhitespaceAndLocaleIndependentTextOnlyNormalization() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            Assertions.assertEquals("istanbul café climate", TextNormalizer.normalize(
                " \tISTANBUL\u00a0CAFE\u0301\u2003Climate \r\n"));
            Assertions.assertEquals("", TextNormalizer.normalize("\u00a0\u2003\t"));
            Evidence evidence = new Evidence("MixedCase-ID", new Source("SOURCE", "Report"),
                "UPPER Text");
            TextNormalizer.normalize(evidence.getContent());
            Assertions.assertEquals("MixedCase-ID", evidence.getId());
            Assertions.assertEquals("UPPER Text", evidence.getContent());
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void testStableTieBreakAtTopKBoundaryAndLiteralQuery() throws Exception {
        Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        for (String id : Arrays.asList("z", "c", "b", "a")) {
            documents.put(id, Collections.singletonMap("evidence", "climate warming evidence"));
        }
        try (Bm25Index index = new Bm25Index(documents)) {
            List<Bm25Index.Hit> hits = index.search("CLIMATE", 2);
            Assertions.assertEquals(Arrays.asList("a", "b"), ids(hits));
            Assertions.assertTrue(hits.get(0).getScore() > 0);
            Assertions.assertEquals(hits.get(0).getScore(), hits.get(1).getScore());
            Assertions.assertEquals(1, hits.get(0).getRank());
            Assertions.assertEquals(2, hits.get(1).getRank());
            Assertions.assertEquals(ids(hits), ids(index.search("climate", 2)));
            Assertions.assertTrue(index.search("*:*", 20).isEmpty());
            Assertions.assertTrue(index.search("unmatchedterm", 20).isEmpty());
            Assertions.assertTrue(index.search(" \u00a0 ", 20).isEmpty());
            Assertions.assertThrows(IllegalArgumentException.class, () -> index.search("x", 0));
        }
    }

    @Test
    public void testFieldMatchesCorpusReplacementAndLifecycle() throws Exception {
        Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("subject", "Alice");
        fields.put("predicate", "residence");
        fields.put("object", "Paris");
        fields.put("evidence", "official report");
        documents.put("first", fields);
        Bm25Index index = new Bm25Index(documents);
        try {
            for (String query : Arrays.asList("alice", "residence", "paris", "official")) {
                Assertions.assertEquals(Collections.singletonList("first"),
                    ids(index.search(query, 20)));
            }
        } finally {
            index.close();
        }
        index.close();
        Assertions.assertThrows(IllegalStateException.class, () -> index.search("alice", 20));
        try (Bm25Index replacement = new Bm25Index(Collections.singletonMap("second",
            Collections.singletonMap("evidence", "Berlin")))) {
            Assertions.assertTrue(replacement.search("paris", 20).isEmpty());
            Assertions.assertEquals(Collections.singletonList("second"),
                ids(replacement.search("berlin", 20)));
        }
        try (Bm25Index empty = new Bm25Index(Collections.emptyMap())) {
            Assertions.assertTrue(empty.search("anything", 20).isEmpty());
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Bm25Index(
            Collections.singletonMap("id", Collections.singletonMap("unexpected", "x"))));
    }

    @Test
    public void testEvidenceIdentityValidationAndScores() throws Exception {
        Evidence first = new Evidence("e-a", new Source("s", "Source"), "warming climate");
        Evidence second = new Evidence("e-b", first.getSource(), "warming climate");
        try (EvidenceRetriever retriever = new EvidenceRetriever(Arrays.asList(second, first, first))) {
            List<EvidenceCandidate> hits = retriever.search("warming", 20);
            Assertions.assertEquals(Arrays.asList("e-a", "e-b"), hits.stream()
                .map(hit -> hit.getEvidence().getId()).collect(Collectors.toList()));
            Assertions.assertTrue(hits.get(0).getVersionIds().isEmpty());
            Assertions.assertTrue(hits.get(0).getScore() > 0);
        }
        Evidence reused = new Evidence("e-a", first.getSource(), "different text");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new EvidenceRetriever(Arrays.asList(first, reused)));
    }

    @Test
    public void testCandidateStrategiesPrioritizeExactBodyAndLimitTotalEvidence() throws Exception {
        final String text = "the earth climate temperature is rising across the planet each year";
        List<Evidence> corpus = new ArrayList<>();
        for (int i = 24; i >= 0; i--) {
            corpus.add(new Evidence(String.format("exact-%02d", i), new Source("s", "unrelated"),
                " \tTHE earth climate temperature is rising across the planet each year\n"));
        }
        corpus.add(new Evidence("a-title-only", new Source("t", text), "unrelated statement"));
        corpus.add(new Evidence("a-soft", new Source("u", "unrelated"), text + " in observations"));
        for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
            RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL)) {
            try (EvidenceRetriever retriever = new EvidenceRetriever(corpus, profile)) {
                List<EvidenceCandidate> hits = retriever.search(text, 20);
                Assertions.assertEquals(20, hits.size());
                Assertions.assertEquals("exact-00", hits.get(0).getEvidence().getId());
                for (EvidenceCandidate hit : hits) {
                    Assertions.assertTrue(hit.getEvidence().getId().startsWith("exact-"));
                    Assertions.assertEquals(1.0, hit.getChannelScores().get("RULE"));
                    Assertions.assertTrue(hit.getChannelRanks().containsKey("RULE"));
                    Assertions.assertTrue(hit.getChannelRanks().containsKey("RRF"));
                }
                Assertions.assertTrue(retriever.search(" \u00a0 ", 20).isEmpty());
                Assertions.assertThrows(IllegalArgumentException.class,
                    () -> retriever.search(text, 0));
            }
        }
        EvidenceRetriever closed = new EvidenceRetriever(corpus, RetrievalProfile.RULE_ONLY);
        closed.close();
        closed.close();
        Assertions.assertThrows(IllegalStateException.class, () -> closed.search(text, 20));
    }

    @Test
    public void testMinHashChannelIsAvailableWithoutBm25AndDoesNotAcceptOnlySharedWords()
        throws Exception {
        final String text = "the earth climate temperature is rising across the planet each year";
        final Evidence exact = new Evidence("exact", new Source("s", "report"), text);
        final Evidence reordered = new Evidence("different", exact.getSource(),
            "year each planet the across rising is temperature climate earth the");
        try (EvidenceRetriever retriever = new EvidenceRetriever(Arrays.asList(exact, reordered),
            RetrievalProfile.RULE_MINHASH)) {
            List<EvidenceCandidate> result = retriever.search(text, 20);
            Assertions.assertEquals(1, result.size());
            Assertions.assertTrue(result.get(0).getChannelScores().containsKey("MINHASH"));
            Assertions.assertFalse(result.get(0).getChannelScores().containsKey("BM25"));
        }
    }

    private static List<String> ids(List<Bm25Index.Hit> hits) {
        return hits.stream().map(Bm25Index.Hit::getId).collect(Collectors.toList());
    }
}
