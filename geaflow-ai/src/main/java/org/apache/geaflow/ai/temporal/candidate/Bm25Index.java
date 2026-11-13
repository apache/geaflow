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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** A fixed, immutable corpus; callers rebuild it from the visible temporal slice. */
public final class Bm25Index implements AutoCloseable {

    public static final int DEFAULT_TOP_K = 20;
    public static final float K1 = 1.2f;
    public static final float B = 0.75f;
    private static final Sort ORDER = new Sort(SortField.FIELD_SCORE,
        new SortField("sort_id", SortField.Type.STRING));

    private static final Comparator<Candidate> CANDIDATE_ORDER = Comparator
        .comparingDouble((Candidate candidate) -> candidate.score).reversed()
        .thenComparing(candidate -> candidate.id);
    private final RetrievalProfile profile;
    private final ChannelIndex rawIndex;
    private final ChannelIndex aliasIndex;
    private boolean closed;

    public Bm25Index(Map<String, Map<String, String>> documents) throws IOException {
        this(documents, RetrievalProfile.BASELINE);
    }

    public Bm25Index(Map<String, Map<String, String>> documents, RetrievalProfile profile)
        throws IOException {
        Objects.requireNonNull(documents, "documents");
        this.profile = Objects.requireNonNull(profile, "profile");
        final ChannelIndex openedRaw = new ChannelIndex(documents, profile, false);
        ChannelIndex openedAlias = null;
        try {
            if (profile.usesLexicalFusion()) {
                openedAlias = new ChannelIndex(documents, RetrievalProfile.ENGLISH, true);
            }
            rawIndex = openedRaw;
            aliasIndex = openedAlias;
        } catch (IOException | RuntimeException | Error exception) {
            closeAfterFailure(exception, openedRaw, openedAlias);
            throw exception;
        }
    }

    public List<Hit> search(String text, int topK) throws IOException {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        if (!profile.usesLexicalFusion()) {
            return rawIndex.search(text, topK);
        }
        final int poolSize = profile.getCandidatePoolSize(topK);
        final Map<String, Candidate> candidates = new TreeMap<>();
        addChannel(candidates, rawIndex.search(text, poolSize), "BM25");
        addChannel(candidates, aliasIndex.search(LexicalTerms.canonicalize(text), poolSize),
            "ALIAS_BM25");
        final List<Candidate> ordered = new ArrayList<>(candidates.values());
        ordered.sort(CANDIDATE_ORDER);
        for (int index = 0; index < ordered.size(); index++) {
            Candidate candidate = ordered.get(index);
            candidate.scores.put("RRF", candidate.score);
            candidate.ranks.put("RRF", index + 1);
        }
        List<Hit> result = new ArrayList<>();
        for (Candidate candidate : ordered.subList(0, Math.min(topK, ordered.size()))) {
            int rank = result.size() + 1;
            result.add(new Hit(candidate.id, candidate.score, rank,
                candidate.scores, candidate.ranks));
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            IOUtils.close(rawIndex, aliasIndex);
        }
    }

    private static void addChannel(Map<String, Candidate> candidates, List<Hit> hits,
                                   String channel) {
        for (Hit hit : hits) {
            Candidate candidate = candidates.computeIfAbsent(hit.id, Candidate::new);
            candidate.scores.put(channel, hit.score);
            candidate.ranks.put(channel, hit.rank);
            candidate.score += 1.0 / (RetrievalProfile.RRF_K + hit.rank);
        }
    }

    private static void closeAfterFailure(Throwable failure, Closeable... resources) {
        try {
            IOUtils.close(resources);
        } catch (IOException | RuntimeException closingFailure) {
            failure.addSuppressed(closingFailure);
        }
    }

    private static final class ChannelIndex implements Closeable {

        private final ByteBuffersDirectory directory;
        private final Analyzer analyzer;
        private final Map<String, Float> fieldWeights;
        private final String[] fields;
        private final DirectoryReader reader;
        private final IndexSearcher searcher;
        private final boolean canonical;

        private ChannelIndex(Map<String, Map<String, String>> documents, RetrievalProfile profile,
                             boolean canonical) throws IOException {
            this.canonical = canonical;
            fieldWeights = profile.getFieldWeights();
            fields = fieldWeights.keySet().toArray(new String[0]);
            directory = new ByteBuffersDirectory();
            Analyzer openedAnalyzer = null;
            DirectoryReader openedReader = null;
            try {
                openedAnalyzer = profile.createAnalyzer();
                openedReader = openReader(documents, openedAnalyzer);
                searcher = new IndexSearcher(openedReader);
                searcher.setSimilarity(new BM25Similarity(K1, B));
                reader = openedReader;
                analyzer = openedAnalyzer;
            } catch (IOException | RuntimeException | Error exception) {
                closeAfterFailure(exception, openedReader, openedAnalyzer, directory);
                throw exception;
            }
        }

        private DirectoryReader openReader(Map<String, Map<String, String>> documents,
                                           Analyzer openedAnalyzer) throws IOException {
            IndexWriterConfig config = new IndexWriterConfig(openedAnalyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            config.setSimilarity(new BM25Similarity(K1, B));
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (Map.Entry<String, Map<String, String>> entry
                    : new TreeMap<>(documents).entrySet()) {
                    writer.addDocument(document(entry.getKey(), entry.getValue()));
                }
            }
            return DirectoryReader.open(directory);
        }

        private Document document(String id, Map<String, String> values) {
            Objects.requireNonNull(id, "document id");
            if (id.trim().isEmpty()) {
                throw new IllegalArgumentException("document id must not be blank");
            }
            Objects.requireNonNull(values, "fields");
            for (String field : values.keySet()) {
                if (!fieldWeights.containsKey(field)) {
                    throw new IllegalArgumentException("Unknown retrieval field: " + field);
                }
            }
            Document document = new Document();
            document.add(new StringField("id", id, Field.Store.YES));
            document.add(new SortedDocValuesField("sort_id", new BytesRef(id)));
            for (String field : fields) {
                if (values.containsKey(field)) {
                    String value = values.get(field);
                    String indexed = canonical ? LexicalTerms.canonicalize(value)
                        : TextNormalizer.normalize(value);
                    document.add(new TextField(field, indexed, Field.Store.NO));
                }
            }
            return document;
        }

        private List<Hit> search(String text, int topK) throws IOException {
            String normalized = TextNormalizer.normalize(text);
            if (normalized.isEmpty() || reader.numDocs() == 0) {
                return Collections.emptyList();
            }
            Query query;
            try {
                query = new MultiFieldQueryParser(fields, analyzer, fieldWeights)
                    .parse(QueryParser.escape(normalized));
            } catch (ParseException exception) {
                throw new IllegalArgumentException("Invalid retrieval text", exception);
            }
            TopDocs matches = searcher.search(query, Math.min(topK, reader.numDocs()), ORDER, true);
            List<Hit> result = new ArrayList<>();
            for (ScoreDoc match : matches.scoreDocs) {
                result.add(new Hit(searcher.doc(match.doc).get("id"), match.score, result.size() + 1));
            }
            return Collections.unmodifiableList(result);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(reader, analyzer, directory);
        }
    }

    private static final class Candidate {

        private final String id;
        private final Map<String, Double> scores = new LinkedHashMap<>();
        private final Map<String, Integer> ranks = new LinkedHashMap<>();
        private double score;

        private Candidate(String id) {
            this.id = id;
        }
    }

    public static final class Hit {

        private final String id;
        private final double score;
        private final int rank;
        private final Map<String, Double> channelScores;
        private final Map<String, Integer> channelRanks;

        private Hit(String id, double score, int rank) {
            this(id, score, rank, Collections.singletonMap("BM25", score),
                Collections.singletonMap("BM25", rank));
        }

        Hit(String id, double score, int rank, Map<String, Double> scores,
            Map<String, Integer> ranks) {
            this.id = id;
            this.score = score;
            this.rank = rank;
            channelScores = Collections.unmodifiableMap(new LinkedHashMap<>(scores));
            channelRanks = Collections.unmodifiableMap(new LinkedHashMap<>(ranks));
        }

        public String getId() {
            return id;
        }

        public double getScore() {
            return score;
        }

        public int getRank() {
            return rank;
        }

        public Map<String, Double> getChannelScores() {
            return channelScores;
        }

        public Map<String, Integer> getChannelRanks() {
            return channelRanks;
        }
    }
}
