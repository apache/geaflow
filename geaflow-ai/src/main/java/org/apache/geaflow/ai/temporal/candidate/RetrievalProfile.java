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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/** Retrieval channels used by temporal candidates and the evidence baseline. */
public enum RetrievalProfile {

    BASELINE,
    ENGLISH,
    LEXICAL_FUSION,
    RULE_ONLY,
    RULE_BM25,
    RULE_MINHASH,
    RULE_ALL;

    public static final int LEXICAL_POOL_SIZE = 100;
    public static final int RRF_K = 60;
    private final Map<String, Float> fieldWeights;

    RetrievalProfile() {
        Map<String, Float> weights = new LinkedHashMap<>();
        weights.put("subject", 1.0f);
        weights.put("predicate", 1.0f);
        weights.put("object", 1.0f);
        weights.put("evidence", 1.0f);
        fieldWeights = Collections.unmodifiableMap(weights);
    }

    public String getAnalyzerName() {
        return this == BASELINE ? StandardAnalyzer.class.getName() : EnglishAnalyzer.class.getName();
    }

    public boolean usesLexicalFusion() {
        return this == LEXICAL_FUSION;
    }

    public boolean isCandidateStrategy() {
        return this == RULE_ONLY || this == RULE_BM25
            || this == RULE_MINHASH || this == RULE_ALL;
    }

    public boolean usesBm25() {
        return this != RULE_ONLY && this != RULE_MINHASH;
    }

    public boolean usesMinHash() {
        return this == RULE_MINHASH || this == RULE_ALL;
    }

    public int getCandidatePoolSize(int topK) {
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        return usesLexicalFusion() || isCandidateStrategy()
            ? Math.max(LEXICAL_POOL_SIZE, topK) : topK;
    }

    public Map<String, Float> getFieldWeights() {
        return fieldWeights;
    }

    public Analyzer createAnalyzer() {
        return this == BASELINE ? new StandardAnalyzer() : new EnglishAnalyzer();
    }
}
