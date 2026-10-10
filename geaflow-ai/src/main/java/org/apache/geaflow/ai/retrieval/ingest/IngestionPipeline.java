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

package org.apache.geaflow.ai.retrieval.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.index.Bm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.IndexArtifact;
import org.apache.geaflow.ai.retrieval.index.VectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.metadata.ImportMetadata;
import org.apache.geaflow.ai.retrieval.metadata.QualityCounters;
import org.apache.geaflow.ai.retrieval.model.document.SourceDocument;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;

/** Deterministic orchestration boundary for source, graph, and index builders. */
public final class IngestionPipeline {

    private final SourceLoader sourceLoader;
    private final DocumentNormalizer normalizer;
    private final EntityExtractor entityExtractor;
    private final GraphWriter graphWriter;
    private final Bm25IndexBuilder bm25Builder;
    private final VectorIndexBuilder vectorBuilder;
    private final MetadataPublisher metadataPublisher;

    public IngestionPipeline(SourceLoader sourceLoader, DocumentNormalizer normalizer,
                             EntityExtractor entityExtractor, GraphWriter graphWriter,
                             Bm25IndexBuilder bm25Builder, VectorIndexBuilder vectorBuilder,
                             MetadataPublisher metadataPublisher) {
        this.sourceLoader = Objects.requireNonNull(sourceLoader, "sourceLoader");
        this.normalizer = Objects.requireNonNull(normalizer, "normalizer");
        this.entityExtractor = Objects.requireNonNull(entityExtractor, "entityExtractor");
        this.graphWriter = Objects.requireNonNull(graphWriter, "graphWriter");
        this.bm25Builder = Objects.requireNonNull(bm25Builder, "bm25Builder");
        this.vectorBuilder = Objects.requireNonNull(vectorBuilder, "vectorBuilder");
        this.metadataPublisher = Objects.requireNonNull(metadataPublisher, "metadataPublisher");
    }

    /** Runs one attempt and publishes only after every artifact has been built successfully. */
    public ImportMetadata run(IngestionContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        List<GraphArtifact> graphArtifacts = new ArrayList<>();
        List<IndexArtifact> indexArtifacts = new ArrayList<>();
        ImportAttempt attempt = null;
        try {
            attempt = metadataPublisher.begin(context);
            Objects.requireNonNull(attempt, "metadata publisher returned a null attempt");
            List<SourceDocument> documents = requireList(sourceLoader.load(context), "documents");
            List<TextChunk> chunks = withSourceUris(documents,
                requireList(normalizer.normalize(documents, context), "chunks"));
            ExtractionResult extraction = Objects.requireNonNull(
                entityExtractor.extract(chunks, context), "extraction");
            GraphArtifact graph = Objects.requireNonNull(
                graphWriter.write(context, documents, chunks, extraction), "graph artifact");
            graphArtifacts.add(graph);
            IndexArtifact bm25 = Objects.requireNonNull(bm25Builder.build(context, chunks), "bm25 artifact");
            indexArtifacts.add(bm25);
            IndexArtifact vector = Objects.requireNonNull(vectorBuilder.build(context, chunks), "vector artifact");
            indexArtifacts.add(vector);
            context.setQualityCounters(new QualityCounters(documents.size(), chunks.size(),
                extraction.getEntities().size(), extraction.getEdges().size(), 0, 0));
            return metadataPublisher.publish(context, attempt, graph,
                Collections.unmodifiableList(indexArtifacts));
        } catch (Exception failure) {
            closeAll(indexArtifacts, failure);
            indexArtifacts.clear();
            closeAll(graphArtifacts, failure);
            graphArtifacts.clear();
            if (attempt != null) {
                try {
                    metadataPublisher.fail(context, attempt, failure);
                } catch (Exception publicationFailure) {
                    failure.addSuppressed(publicationFailure);
                }
            }
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            throw new IOException("ingestion failed", failure);
        } finally {
            closeAll(indexArtifacts, null);
            closeAll(graphArtifacts, null);
        }
    }

    private static <T> List<T> requireList(List<T> values, String name) {
        Objects.requireNonNull(values, name);
        if (values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(name + " must not contain null");
        }
        return values;
    }

    private static List<TextChunk> withSourceUris(List<SourceDocument> documents, List<TextChunk> chunks) {
        Map<String, SourceDocument> byId = new HashMap<>();
        for (SourceDocument document : documents) {
            if (byId.put(document.getDocumentId(), document) != null) {
                throw new IllegalArgumentException("duplicate source document ID");
            }
        }
        List<TextChunk> result = new ArrayList<>(chunks.size());
        for (TextChunk chunk : chunks) {
            SourceDocument document = byId.get(chunk.getDocumentId());
            if (document == null || document.getSourceUri() == null
                || document.getSourceUri().trim().isEmpty()) {
                throw new IllegalArgumentException("chunk requires a source document with a source URI");
            }
            if (chunk.getSourceUri() != null && !document.getSourceUri().equals(chunk.getSourceUri())) {
                throw new IllegalArgumentException("chunk source URI does not match its document");
            }
            result.add(chunk.withSourceUri(document.getSourceUri()));
        }
        return result;
    }

    private static void closeAll(List<? extends AutoCloseable> resources, Exception failure)
        throws IOException {
        IOException closeFailure = null;
        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception closeError) {
                if (closeFailure == null) {
                    closeFailure = new IOException("failed to close ingestion artifact", closeError);
                } else {
                    closeFailure.addSuppressed(closeError);
                }
            }
        }
        if (closeFailure != null && failure == null) {
            throw closeFailure;
        }
        if (closeFailure != null && failure != null) {
            failure.addSuppressed(closeFailure);
        }
    }
}
