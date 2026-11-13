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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.index.ArtifactIdentity;
import org.apache.geaflow.ai.retrieval.index.ChannelSearchResult;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/** Reusable bounded reader for a published Lucene BM25 artifact. */
public final class Bm25IndexReader implements Closeable {
    private final Directory directory;
    private final DirectoryReader reader;
    private final IndexSearcher searcher;
    private final ArtifactIdentity identity;

    public Bm25IndexReader(Path artifact) {
        this(artifact, null, null);
    }

    public Bm25IndexReader(Path artifact, String expectedGraphVersion, String expectedIndexVersion) {
        Directory openedDirectory = null;
        DirectoryReader openedReader = null;
        try {
            if (artifact == null || !Files.isDirectory(artifact)) {
                throw new IOException("missing BM25 artifact: " + artifact);
            }
            openedDirectory = FSDirectory.open(artifact);
            openedReader = DirectoryReader.open(openedDirectory);
            identity = ArtifactIdentity.fromMap(openedReader.getIndexCommit().getUserData());
            identity.validateVersions(expectedGraphVersion, expectedIndexVersion);
            directory = openedDirectory;
            reader = openedReader;
            searcher = new IndexSearcher(reader);
        } catch (IOException | RuntimeException error) {
            closeOnFailure(openedReader, error);
            closeOnFailure(openedDirectory, error);
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, "unable to open BM25 artifact", error);
        }
    }

    private static void closeOnFailure(Closeable resource, Exception failure) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException error) {
                failure.addSuppressed(error);
            }
        }
    }

    public ArtifactIdentity getIdentity() {
        return identity;
    }

    public List<Bm25Hit> search(String queryText, int candidateLimit, int topK) {
        return search(queryText, candidateLimit, topK, 0L);
    }

    public List<Bm25Hit> search(String queryText, int candidateLimit, int topK, long deadlineNanos) {
        ChannelSearchResult<Bm25Hit> found = searchWithStats(queryText, candidateLimit, topK, deadlineNanos);
        return new ArrayList<>(found.getHits().subList(0, Math.min(topK, found.getHits().size())));
    }

    public ChannelSearchResult<Bm25Hit> searchWithStats(String queryText, int candidateLimit,
                                                       int topK, long deadlineNanos) {
        return searchWithStats(queryText, candidateLimit, topK, deadlineNanos, System::nanoTime);
    }

    /** Counts documents scored, rather than the number of hits returned by Lucene. */
    public ChannelSearchResult<Bm25Hit> searchWithStats(String queryText, int candidateLimit,
                                                       int topK, long deadlineNanos, LongSupplier clock) {
        if (queryText == null || queryText.trim().isEmpty() || candidateLimit < 1 || topK < 1) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid BM25 query or budget");
        }
        BoundedCollector collector = new BoundedCollector(candidateLimit, deadlineNanos, clock);
        try (StandardAnalyzer analyzer = new StandardAnalyzer()) {
            collector.checkDeadline();
            Query query = new QueryParser("text", analyzer).parse(queryText);
            collector.checkDeadline();
            searcher.search(query, collector);
            collector.checkDeadline();
        } catch (SearchStopped stopped) {
            // The collector preserves all fully evaluated hits and the typed stopping reason.
        } catch (org.apache.lucene.queryparser.classic.ParseException error) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid BM25 query", error);
        } catch (IOException | RuntimeException error) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, "BM25 artifact search failed", error);
        }
        collector.hits.sort(Comparator.comparing(Bm25Hit::getScore).reversed()
            .thenComparing(Bm25Hit::getDocumentId).thenComparing(Bm25Hit::getChunkId));
        List<Bm25Hit> result = new ArrayList<>();
        for (Bm25Hit hit : collector.hits) {
            result.add(new Bm25Hit(hit.getChunkId(), hit.getDocumentId(), hit.getScore(), result.size() + 1, hit.getText()));
        }
        return new ChannelSearchResult<>(result, collector.evaluated, collector.reason);
    }

    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } finally {
            directory.close();
        }
    }

    private final class BoundedCollector extends SimpleCollector {
        private final int limit;
        private final long deadline;
        private final LongSupplier clock;
        private final List<Bm25Hit> hits = new ArrayList<>();
        private final Set<String> chunkIds = new HashSet<>();
        private Scorable scorer;
        private int docBase;
        private int evaluated;
        private RecallStopReason reason = RecallStopReason.COMPLETED;

        private BoundedCollector(int limit, long deadline, LongSupplier clock) {
            this.limit = limit;
            this.deadline = deadline;
            this.clock = clock;
        }

        private void checkDeadline() {
            if (deadline != 0L && clock.getAsLong() - deadline >= 0L) {
                reason = RecallStopReason.DEADLINE;
                throw new SearchStopped();
            }
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) {
            checkDeadline();
            docBase = context.docBase;
        }

        @Override
        public void setScorer(Scorable scorer) {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            checkDeadline();
            if (evaluated >= limit) {
                reason = RecallStopReason.CANDIDATE_BUDGET;
                throw new SearchStopped();
            }
            evaluated++;
            float score = scorer.score();
            Document document = searcher.doc(docBase + doc);
            String chunkId = document.get("chunkId");
            String documentId = document.get("documentId");
            String text = document.get("text");
            if (chunkId == null || documentId == null || text == null || !Float.isFinite(score)
                || !chunkIds.add(chunkId)) {
                throw new IOException("invalid or duplicate BM25 document");
            }
            hits.add(new Bm25Hit(chunkId, documentId, score, 0, text));
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }
    }

    private static final class SearchStopped extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
