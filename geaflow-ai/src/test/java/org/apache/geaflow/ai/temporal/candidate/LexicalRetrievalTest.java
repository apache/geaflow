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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LexicalRetrievalTest {

    @Test
    public void testFixedAliasesHaveBoundariesAndAreIdempotent() {
        final String original = "CO₂ CO2 CO 2 carbon dioxide CH4 methane N2O nitrous oxide";
        String canonical = LexicalTerms.canonicalize(original);
        Assertions.assertEquals("carbon dioxide carbon dioxide carbon dioxide carbon dioxide "
            + "methane methane nitrous oxide nitrous oxide", canonical);
        Assertions.assertEquals(canonical, LexicalTerms.canonicalize(canonical));
        Assertions.assertEquals("CO₂ CO2 CO 2 carbon dioxide CH4 methane N2O nitrous oxide",
            original);
        Assertions.assertEquals("fooco2 co20 xch4 n2o2 carbon dioxides _co2 co2_",
            LexicalTerms.canonicalize("fooCO2 CO20 xCH4 N2O2 carbon dioxides _CO2 CO2_"));
        Assertions.assertEquals("residency time lifetime",
            LexicalTerms.canonicalize("residency time lifetime"));
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> LexicalTerms.getAliases().put("custom", "other"));
    }

    @Test
    public void testRawChannelRetainsEnglishScoresAndAliasChannelAddsCandidates() throws Exception {
        final Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        documents.put("formula", fields("CO2 warms atmosphere"));
        documents.put("name", fields("Carbon dioxide warms atmosphere"));
        documents.put("methane", fields("Methane traps heat"));
        try (Bm25Index english = new Bm25Index(documents, RetrievalProfile.ENGLISH);
             Bm25Index fusion = new Bm25Index(documents, RetrievalProfile.LEXICAL_FUSION)) {
            final List<Bm25Index.Hit> raw = english.search("CO2", 100);
            final Map<String, Bm25Index.Hit> fused = byId(fusion.search("CO2", 20));
            Assertions.assertEquals(Collections.singletonList("formula"), ids(raw));
            Assertions.assertEquals(new HashSet<>(Arrays.asList("formula", "name")),
                fused.keySet());
            Assertions.assertEquals(raw.get(0).getScore(),
                fused.get("formula").getChannelScores().get("BM25").doubleValue());
            Assertions.assertEquals(Integer.valueOf(raw.get(0).getRank()),
                fused.get("formula").getChannelRanks().get("BM25"));
            Assertions.assertFalse(fused.get("name").getChannelScores().containsKey("BM25"));
            Assertions.assertTrue(fused.get("name").getChannelScores()
                .containsKey("ALIAS_BM25"));
            Assertions.assertEquals(Collections.singletonList("methane"),
                ids(fusion.search("CH4", 20)));
            Assertions.assertEquals("CO2 warms atmosphere", documents.get("formula").get("evidence"));
        }
    }

    @Test
    public void testFusionDeduplicatesAndKeepsStableIdTiesAndImmutableChannels() throws Exception {
        final Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        for (String id : Arrays.asList("z", "b", "a")) {
            documents.put(id, fields("carbon dioxide atmospheric evidence"));
        }
        try (Bm25Index fusion = new Bm25Index(documents, RetrievalProfile.LEXICAL_FUSION)) {
            List<Bm25Index.Hit> hits = fusion.search("carbon dioxide", 2);
            Assertions.assertEquals(Arrays.asList("a", "b"), ids(hits));
            Assertions.assertEquals(ids(hits), ids(fusion.search("carbon dioxide", 2)));
            Assertions.assertEquals(2.0 / (RetrievalProfile.RRF_K + 1), hits.get(0).getScore());
            Assertions.assertEquals(2, hits.get(1).getRank());
            Assertions.assertThrows(UnsupportedOperationException.class,
                () -> hits.get(0).getChannelScores().put("other", 0.0));
            Assertions.assertThrows(UnsupportedOperationException.class,
                () -> hits.get(0).getChannelRanks().put("other", 0));
            Assertions.assertTrue(fusion.search("unmatchedterm", 20).isEmpty());
            Assertions.assertThrows(IllegalArgumentException.class,
                () -> fusion.search("carbon", 0));
        }
    }

    @Test
    public void testLexicalProfilesDoNotIndexTitlesAndCloseBothChannels() throws Exception {
        final RetrievalProfile profile = RetrievalProfile.LEXICAL_FUSION;
        Assertions.assertEquals(100, profile.getCandidatePoolSize(20));
        Assertions.assertEquals(150, profile.getCandidatePoolSize(150));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Bm25Index(
            Collections.singletonMap("title", Collections.singletonMap("title", "CO2")),
            profile));
        Bm25Index index = new Bm25Index(Collections.singletonMap("body",
            fields("carbon dioxide")), profile);
        Assertions.assertEquals(Collections.singletonList("body"),
            ids(index.search("CO2", 20)));
        index.close();
        index.close();
        Assertions.assertThrows(IllegalStateException.class, () -> index.search("CO2", 20));
    }

    @Test
    public void testRequestedDepthBeyondPoolMinimumAndEmptyAnalyzedQueries() throws Exception {
        Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        for (int index = 0; index < 155; index++) {
            documents.put(String.format("document-%03d", index), fields("carbon dioxide atmosphere"));
        }
        final RetrievalProfile profile = RetrievalProfile.LEXICAL_FUSION;
        try (Bm25Index index = new Bm25Index(documents, profile)) {
            List<Bm25Index.Hit> hits = index.search("CO2", 150);
            Assertions.assertEquals(150, hits.size());
            Assertions.assertEquals("document-000", hits.get(0).getId());
            Assertions.assertEquals("document-149", hits.get(149).getId());
            Assertions.assertEquals(150, hits.get(149).getRank());
            Assertions.assertEquals(20, index.search("CO2", 20).size());
            Assertions.assertTrue(index.search("the and of", 20).isEmpty());
            Assertions.assertTrue(index.search("", 20).isEmpty());
            Assertions.assertTrue(index.search("!!!", 20).isEmpty());
        }
    }

    private static Map<String, String> fields(String text) {
        return Collections.singletonMap("evidence", text);
    }

    private static List<String> ids(List<Bm25Index.Hit> hits) {
        return hits.stream().map(Bm25Index.Hit::getId).collect(Collectors.toList());
    }

    private static Map<String, Bm25Index.Hit> byId(List<Bm25Index.Hit> hits) {
        return hits.stream().collect(Collectors.toMap(Bm25Index.Hit::getId, hit -> hit));
    }
}
