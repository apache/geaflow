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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.index.Bm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.IndexArtifact;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.IndexBuildMetadata;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/** Builds a deterministic Lucene 8 BM25 artifact from canonical chunks. */
public final class LuceneBm25IndexBuilder implements Bm25IndexBuilder {

    private static final String INDEX_NAME = "bm25";
    private static final String BUILDER_VERSION = "lucene-8.11.2-standard-v1";
    private final Path outputDirectory;

    public LuceneBm25IndexBuilder(Path outputDirectory) {
        this.outputDirectory = Objects.requireNonNull(outputDirectory, "outputDirectory");
    }

    @Override
    public IndexArtifact build(IngestionContext context, List<TextChunk> chunks) throws IOException {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(chunks, "chunks");
        List<TextChunk> ordered = new ArrayList<>(chunks);
        ordered.sort(Comparator.comparing(TextChunk::getChunkId));
        Files.createDirectories(outputDirectory);
        String version = context.getGraphVersion().getVersion();
        Path staging = Files.createTempDirectory(outputDirectory, INDEX_NAME + "-" + version + "-");
        Path published = outputDirectory.resolve(INDEX_NAME + "-" + version);
        try (Directory directory = FSDirectory.open(staging)) {
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            config.setSimilarity(new org.apache.lucene.search.similarities.BM25Similarity());
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (TextChunk chunk : ordered) {
                    Document document = new Document();
                    document.add(new StringField("chunkId", chunk.getChunkId(), Field.Store.YES));
                    document.add(new StringField("documentId", chunk.getDocumentId(), Field.Store.YES));
                    document.add(new TextField("text", chunk.getText(), Field.Store.YES));
                    document.add(new StoredField("startOffset", chunk.getStartOffset()));
                    document.add(new StoredField("endOffset", chunk.getEndOffset()));
                    if (chunk.getTextHash() != null) {
                        document.add(new StoredField("textHash", chunk.getTextHash()));
                    }
                    writer.addDocument(document);
                }
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                if (reader.numDocs() != ordered.size()) {
                    throw new IOException("BM25 document count mismatch");
                }
            }
        }
        if (Files.exists(published)) {
            return new LuceneIndexArtifact(new IndexBuildMetadata(context.getGraphVersion(),
                new IndexVersion(INDEX_NAME, version, version), INDEX_NAME, BUILDER_VERSION,
                published.toString(), true), published);
        }
        Files.move(staging, published, StandardCopyOption.ATOMIC_MOVE);
        IndexVersion indexVersion = new IndexVersion(INDEX_NAME, version, version);
        IndexBuildMetadata metadata = new IndexBuildMetadata(context.getGraphVersion(), indexVersion,
            INDEX_NAME, BUILDER_VERSION, published.toString(), true);
        return new LuceneIndexArtifact(metadata, published);
    }

    private static final class LuceneIndexArtifact implements IndexArtifact {
        private final IndexBuildMetadata metadata;
        private final Path path;

        private LuceneIndexArtifact(IndexBuildMetadata metadata, Path path) {
            this.metadata = metadata;
            this.path = path;
        }

        @Override
        public IndexBuildMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void close() throws IOException {
            if (!Files.exists(path.resolve("segments.gen")) && !Files.exists(path.resolve("segments_1"))) {
                throw new IOException("BM25 artifact is not readable: " + path);
            }
        }
    }
}
