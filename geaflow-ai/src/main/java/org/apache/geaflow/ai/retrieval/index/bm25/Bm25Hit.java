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
package org.apache.geaflow.ai.retrieval.index.bm25;

import java.util.Collections;
import java.util.List;

/**
 * A deterministic BM25 match returned by {@link Bm25IndexReader}.
 *
 * <p>The design requires the BM25 channel to return matched terms and IDF statistics alongside the
 * score, so an adaptive router can reason about lexical confidence and an operator can explain a
 * ranking. Both are captured at scoring time and exposed here.
 */
public final class Bm25Hit {
    private final String chunkId;
    private final String documentId;
    private final float score;
    private final int rank;
    private final String text;
    private final List<String> matchedTerms;
    private final double averageIdf;

    public Bm25Hit(String chunkId, String documentId, float score, int rank, String text) {
        this(chunkId, documentId, score, rank, text, Collections.emptyList(), 0.0D);
    }

    /**
     * Creates a hit that reports which query terms matched and how informative they are.
     *
     * @param matchedTerms query terms that actually matched this chunk, in query order
     * @param averageIdf mean inverse document frequency of the matched terms
     */
    public Bm25Hit(String chunkId, String documentId, float score, int rank, String text,
                   List<String> matchedTerms, double averageIdf) {
        if (averageIdf < 0.0D || Double.isNaN(averageIdf)) {
            throw new IllegalArgumentException("averageIdf must be finite and non-negative");
        }
        this.chunkId = chunkId;
        this.documentId = documentId;
        this.score = score;
        this.rank = rank;
        this.text = text;
        this.matchedTerms = matchedTerms == null ? Collections.emptyList()
            : Collections.unmodifiableList(new java.util.ArrayList<>(matchedTerms));
        this.averageIdf = averageIdf;
    }

    public String getChunkId() {
        return chunkId;
    }

    public String getDocumentId() {
        return documentId;
    }

    public float getScore() {
        return score;
    }

    public int getRank() {
        return rank;
    }

    public String getText() {
        return text;
    }

    public List<String> getMatchedTerms() {
        return matchedTerms;
    }

    public double getAverageIdf() {
        return averageIdf;
    }
}