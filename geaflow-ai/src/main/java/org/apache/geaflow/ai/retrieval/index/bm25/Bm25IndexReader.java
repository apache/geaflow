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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

/** Reusable bounded reader for a published Lucene BM25 artifact. */
public final class Bm25IndexReader implements Closeable {
    private final Directory directory;
    private final DirectoryReader reader;
    private final IndexSearcher searcher;
    private final ArtifactIdentity identity;
    /** Cached detection of the optional title/alias surface field; null means not yet probed. */
    private Boolean hasSurfaceField;

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

    /**
     * Returns how many chunk documents this index carries.
     *
     * <p>Index coverage is published in the quality report, so the reader exposes its own document
     * count rather than forcing callers to guess from the artifact directory.
     */
    public int getDocumentCount() {
        return reader.numDocs();
    }

    public List<Bm25Hit> search(String queryText, int candidateLimit, int topK) {
        return search(queryText, candidateLimit, topK, 0L);
    }

    public List<Bm25Hit> search(String queryText, int candidateLimit, int topK, long deadlineNanos) {
        ChannelSearchResult<Bm25Hit> found = searchWithStats(queryText, candidateLimit, topK, deadlineNanos);
        return new ArrayList<>(found.getHits().subList(0, Math.min(topK, found.getHits().size())));
    }

    /**
     * Executes a full Lucene ranking pass and returns the best {@code topK}
     * matches. Unlike {@link #search}, the candidate limit is applied after
     * ranking and does not truncate Lucene's document traversal order.
     */
    public List<Bm25Hit> searchExact(String queryText, int candidateLimit, int topK) {
        return searchExact(queryText, candidateLimit, topK, 0L);
    }

    public List<Bm25Hit> searchExact(String queryText, int candidateLimit, int topK, long deadlineNanos) {
        ChannelSearchResult<Bm25Hit> found = searchExactWithStats(queryText, candidateLimit, topK, deadlineNanos);
        return new ArrayList<>(found.getHits().subList(0, Math.min(topK, found.getHits().size())));
    }

    /** Returns exact Lucene ranking statistics while retaining deadline handling. */
    public ChannelSearchResult<Bm25Hit> searchExactWithStats(String queryText, int candidateLimit,
                                                             int topK, long deadlineNanos) {
        return searchExactWithStats(queryText, candidateLimit, topK, deadlineNanos, System::nanoTime);
    }

    public ChannelSearchResult<Bm25Hit> searchExactWithStats(String queryText, int candidateLimit,
                                                             int topK, long deadlineNanos,
                                                             LongSupplier clock) {
        if (queryText == null || queryText.trim().isEmpty() || candidateLimit < 1 || topK < 1) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid BM25 query or budget");
        }
        BoundedCollector collector = new BoundedCollector(Integer.MAX_VALUE, deadlineNanos, clock);
        try (StandardAnalyzer analyzer = new StandardAnalyzer()) {
            // Matched-term and IDF lookups touch the index, so they must run inside the same guard
            // as the search: a closed reader has to surface as a typed retrieval error.
            collector = new BoundedCollector(Integer.MAX_VALUE, deadlineNanos, clock,
                matchedTerms(queryText));
            collector.checkDeadline();
            Query query = buildQuery(queryText, analyzer);
            collector.checkDeadline();
            searcher.search(query, collector);
            collector.checkDeadline();
        } catch (SearchStopped stopped) {
            // Preserve fully evaluated hits and the deadline reason.
        } catch (org.apache.lucene.queryparser.classic.ParseException error) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid BM25 query", error);
        } catch (IOException | RuntimeException error) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, "BM25 artifact search failed", error);
        }
        collector.hits.sort(Comparator.comparing(Bm25Hit::getScore).reversed()
            .thenComparing(Bm25Hit::getDocumentId).thenComparing(Bm25Hit::getChunkId));
        int resultSize = Math.min(candidateLimit, collector.hits.size());
        List<Bm25Hit> result = new ArrayList<>(resultSize);
        for (int index = 0; index < resultSize; index++) {
            Bm25Hit hit = collector.hits.get(index);
            result.add(new Bm25Hit(hit.getChunkId(), hit.getDocumentId(), hit.getScore(), index + 1,
                hit.getText(), hit.getMatchedTerms(), hit.getAverageIdf()));
        }
        return new ChannelSearchResult<>(result, collector.evaluated, collector.reason);
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
            // Matched-term and IDF lookups touch the index, so they must run inside the same guard
            // as the search: a closed reader has to surface as a typed retrieval error.
            collector = new BoundedCollector(candidateLimit, deadlineNanos, clock,
                matchedTerms(queryText));
            collector.checkDeadline();
            // Retrieval queries are natural language, not Lucene query syntax. buildQuery escapes
            // them, so punctuation in questions (for example "film (Film)?") cannot become a parser
            // operator and turn an otherwise valid request into HTTP 400.
            Query query = buildQuery(queryText, analyzer);
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
            result.add(new Bm25Hit(hit.getChunkId(), hit.getDocumentId(), hit.getScore(),
                result.size() + 1, hit.getText(), hit.getMatchedTerms(), hit.getAverageIdf()));
        }
        return new ChannelSearchResult<>(result, collector.evaluated, collector.reason);
    }

    /**
     * Builds the retrieval query across the chunk body and the supplementary title/alias surface.
     *
     * <p>Both fields are searched with the same analyzer and an optional clause, so a query that
     * matches only a document title or an entity alias still retrieves the chunk. Lexical surfaces
     * produced before this field existed simply contribute nothing.
     */
    private Query buildQuery(String queryText, StandardAnalyzer analyzer)
        throws org.apache.lucene.queryparser.classic.ParseException {
        String escaped = QueryParser.escape(queryText);
        Query body = new QueryParser("text", analyzer).parse(escaped);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(body, BooleanClause.Occur.SHOULD);
        if (hasSurfaceField()) {
            builder.add(new QueryParser(LuceneBm25IndexBuilder.SURFACE_FIELD, analyzer).parse(escaped),
                BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    private boolean hasSurfaceField() {
        if (hasSurfaceField == null) {
            hasSurfaceField = Boolean.FALSE;
            try {
                for (LeafReaderContext context : reader.leaves()) {
                    if (context.reader().getFieldInfos()
                        .fieldInfo(LuceneBm25IndexBuilder.SURFACE_FIELD) != null) {
                        hasSurfaceField = Boolean.TRUE;
                        break;
                    }
                }
            } catch (RuntimeException ignored) {
                // An unreadable field map degrades to body-only search rather than failing.
            }
        }
        return hasSurfaceField;
    }

    /**
     * Returns the query terms that actually matched a chunk, plus their mean IDF.
     *
     * <p>The design requires matched terms and IDF statistics to be reported with every BM25 hit so
     * lexical confidence is auditable. Terms are reported in query order.
     */
    private MatchedTerms matchedTerms(String queryText) {
        Set<String> unique = new LinkedHashSet<>();
        try (StandardAnalyzer analyzer = new StandardAnalyzer()) {
            try (org.apache.lucene.analysis.TokenStream stream = analyzer.tokenStream("text", queryText)) {
                org.apache.lucene.analysis.tokenattributes.CharTermAttribute attribute =
                    stream.addAttribute(org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class);
                stream.reset();
                while (stream.incrementToken()) {
                    unique.add(attribute.toString());
                }
                stream.end();
            }
        } catch (IOException error) {
            return new MatchedTerms(Collections.<String>emptyList(), 0.0D);
        }
        List<String> terms = new ArrayList<>();
        double totalIdf = 0.0D;
        for (String term : unique) {
            double idf = inverseDocumentFrequency(new Term("text", term));
            if (idf > 0.0D) {
                terms.add(term);
                totalIdf += idf;
            }
        }
        return new MatchedTerms(terms, terms.isEmpty() ? 0.0D : totalIdf / terms.size());
    }

    private double inverseDocumentFrequency(Term term) {
        try {
            Terms terms = MultiTerms.getTerms(reader, term.field());
            if (terms == null) {
                return 0.0D;
            }
            TermsEnum iterator = terms.iterator();
            if (!iterator.seekExact(new BytesRef(term.text()))) {
                return 0.0D;
            }
            long docFreq = iterator.docFreq();
            if (docFreq < 1L) {
                return 0.0D;
            }
            int docCount = Math.max(1, reader.numDocs());
            return Math.log(1.0D + (double) docCount / (double) docFreq);
        } catch (IOException error) {
            return 0.0D;
        }
    }

    /** Query terms that survived matching, with their mean inverse document frequency. */
    private static final class MatchedTerms {
        private final List<String> terms;
        private final double averageIdf;

        private MatchedTerms(List<String> terms, double averageIdf) {
            this.terms = Collections.unmodifiableList(new ArrayList<>(terms));
            this.averageIdf = averageIdf;
        }
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
        private final MatchedTerms observedTerms;
        private Scorable scorer;
        private int docBase;
        private int evaluated;
        private RecallStopReason reason = RecallStopReason.COMPLETED;

        private BoundedCollector(int limit, long deadline, LongSupplier clock) {
            this(limit, deadline, clock, new MatchedTerms(Collections.<String>emptyList(), 0.0D));
        }

        private BoundedCollector(int limit, long deadline, LongSupplier clock,
                                 MatchedTerms observedTerms) {
            this.limit = limit;
            this.deadline = deadline;
            this.clock = clock;
            this.observedTerms = observedTerms;
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
            hits.add(new Bm25Hit(chunkId, documentId, score, 0, text, observedTerms.terms,
                observedTerms.averageIdf));
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
