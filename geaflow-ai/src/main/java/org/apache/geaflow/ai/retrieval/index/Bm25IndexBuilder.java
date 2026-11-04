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
package org.apache.geaflow.ai.retrieval.index;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;

/**
 * Builds a versioned lexical index from canonical text chunks.
 *
 * <p>The design requires the BM25 index to cover the title, the chunk text and optional derived
 * lexical surfaces, not the chunk body alone. A body-only index cannot match a title-only query or
 * an entity alias, which is exactly the recall hole the design calls out.
 */
public interface Bm25IndexBuilder {

    /** Builds a body-only index. */
    IndexArtifact build(IngestionContext context, List<TextChunk> chunks) throws IOException;

    /**
     * Builds an index that also covers per-chunk supplementary lexical surfaces.
     *
     * @param chunks canonical chunks in any order
     * @param chunkSurfaces extra searchable text per chunk id: document title and entity aliases
     */
    default IndexArtifact build(IngestionContext context, List<TextChunk> chunks,
                                Map<String, List<String>> chunkSurfaces) throws IOException {
        return build(context, chunks);
    }

    /** Returns an empty supplementary-surface map, for callers that have no extra text. */
    static Map<String, List<String>> noSurfaces() {
        return Collections.emptyMap();
    }
}