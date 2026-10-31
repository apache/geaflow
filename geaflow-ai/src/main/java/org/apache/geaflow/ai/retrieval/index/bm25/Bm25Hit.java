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

/**
 * A deterministic BM25 match returned by {@link Bm25IndexReader}.
 */
public final class Bm25Hit {
    private final String chunkId;
    private final String documentId;
    private final float score;
    private final int rank;
    private final String text;

    public Bm25Hit(String chunkId, String documentId, float score, int rank, String text) {
        this.chunkId = chunkId;
        this.documentId = documentId;
        this.score = score;
        this.rank = rank;
        this.text = text;
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
}
