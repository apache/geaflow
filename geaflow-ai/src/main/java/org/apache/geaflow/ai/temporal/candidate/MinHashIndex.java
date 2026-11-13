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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** An immutable near-duplicate index with LSH recall and exact Jaccard verification. */
public final class MinHashIndex {

    public static final int SHINGLE_SIZE = 3;
    public static final int HASH_COUNT = 128;
    public static final int BANDS = 32;
    public static final int ROWS_PER_BAND = 4;
    public static final long SEED = 0x6a09e667f3bcc909L;
    public static final double JACCARD_THRESHOLD = 0.5;
    public static final String HASH_ALGORITHM = "UTF8_FNV1A64_SEEDED_SPLITMIX64_V1";
    public static final String TOKENIZATION = "NFC_ROOT_LOWERCASE_UNICODE_LETTER_MARK_NUMBER";

    private static final Pattern TOKEN = Pattern.compile("[\\p{L}\\p{M}\\p{N}]+");
    private static final Set<String> FIELDS = Collections.unmodifiableSet(new HashSet<>(
        Arrays.asList("subject", "predicate", "object", "evidence", "title")));
    private static final long HASH_STEP = 0x9e3779b97f4a7c15L;
    private static final Comparator<Bm25Index.Hit> ORDER = Comparator
        .comparingDouble(Bm25Index.Hit::getScore).reversed()
        .thenComparing(Bm25Index.Hit::getId);

    private final Map<String, Set<String>> documentShingles;
    private final Map<BandKey, Set<String>> buckets;

    public MinHashIndex(Map<String, Map<String, String>> documents) {
        Objects.requireNonNull(documents, "documents");
        final Map<String, Set<String>> indexed = new TreeMap<>();
        final Map<BandKey, Set<String>> mutableBuckets = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> document : documents.entrySet()) {
            final String id = Objects.requireNonNull(document.getKey(), "document id");
            if (id.trim().isEmpty()) {
                throw new IllegalArgumentException("document id must not be blank");
            }
            Objects.requireNonNull(document.getValue(), "fields");
            final Set<String> features = new HashSet<>();
            for (Map.Entry<String, String> field : document.getValue().entrySet()) {
                if (!FIELDS.contains(field.getKey())) {
                    throw new IllegalArgumentException("Unknown retrieval field: " + field.getKey());
                }
                features.addAll(shingles(field.getValue()));
            }
            if (!features.isEmpty()) {
                indexed.put(id, Collections.unmodifiableSet(features));
                final long[] signature = signature(features);
                for (int band = 0; band < BANDS; band++) {
                    mutableBuckets.computeIfAbsent(new BandKey(band, signature),
                        key -> new TreeSet<>()).add(id);
                }
            }
        }
        documentShingles = Collections.unmodifiableMap(indexed);
        final Map<BandKey, Set<String>> immutableBuckets = new HashMap<>();
        for (Map.Entry<BandKey, Set<String>> bucket : mutableBuckets.entrySet()) {
            immutableBuckets.put(bucket.getKey(), Collections.unmodifiableSet(bucket.getValue()));
        }
        buckets = Collections.unmodifiableMap(immutableBuckets);
    }

    public List<Bm25Index.Hit> search(String text, int topK) {
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        final Set<String> query = shingles(text);
        if (query.isEmpty()) {
            return Collections.emptyList();
        }
        final Set<String> candidateIds = new TreeSet<>();
        final long[] signature = signature(query);
        for (int band = 0; band < BANDS; band++) {
            candidateIds.addAll(buckets.getOrDefault(new BandKey(band, signature),
                Collections.emptySet()));
        }
        final List<Bm25Index.Hit> ordered = new ArrayList<>();
        for (String id : candidateIds) {
            double score = jaccard(query, documentShingles.get(id));
            if (score >= JACCARD_THRESHOLD) {
                ordered.add(hit(id, score, 1));
            }
        }
        ordered.sort(ORDER);
        final List<Bm25Index.Hit> result = new ArrayList<>();
        for (int index = 0; index < Math.min(topK, ordered.size()); index++) {
            Bm25Index.Hit candidate = ordered.get(index);
            result.add(hit(candidate.getId(), candidate.getScore(), index + 1));
        }
        return Collections.unmodifiableList(result);
    }

    static Set<String> shingles(String text) {
        final Matcher matcher = TOKEN.matcher(TextNormalizer.normalize(text));
        final List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        final Set<String> result = new HashSet<>();
        for (int index = 0; index + SHINGLE_SIZE <= tokens.size(); index++) {
            result.add(tokens.get(index) + " " + tokens.get(index + 1) + " "
                + tokens.get(index + 2));
        }
        return result;
    }

    static double jaccard(Set<String> left, Set<String> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return 0.0;
        }
        int intersection = 0;
        for (String shingle : left) {
            if (right.contains(shingle)) {
                intersection++;
            }
        }
        return (double) intersection / (left.size() + right.size() - intersection);
    }

    static long[] signature(Set<String> shingles) {
        final long[] result = new long[HASH_COUNT];
        Arrays.fill(result, -1L);
        for (String shingle : shingles) {
            long hash = 0xcbf29ce484222325L;
            for (byte value : shingle.getBytes(StandardCharsets.UTF_8)) {
                hash ^= value & 0xffL;
                hash *= 0x100000001b3L;
            }
            for (int index = 0; index < HASH_COUNT; index++) {
                long value = mix(hash + SEED + HASH_STEP * (index + 1));
                if (Long.compareUnsigned(value, result[index]) < 0) {
                    result[index] = value;
                }
            }
        }
        return result;
    }

    private static long mix(long value) {
        long mixed = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        mixed = (mixed ^ (mixed >>> 27)) * 0x94d049bb133111ebL;
        return mixed ^ (mixed >>> 31);
    }

    private static Bm25Index.Hit hit(String id, double score, int rank) {
        return new Bm25Index.Hit(id, score, rank, Collections.singletonMap("MINHASH", score),
            Collections.singletonMap("MINHASH", rank));
    }

    private static final class BandKey {
        private final int band;
        private final long[] values;

        private BandKey(int band, long[] signature) {
            this.band = band;
            values = Arrays.copyOfRange(signature, band * ROWS_PER_BAND,
                (band + 1) * ROWS_PER_BAND);
        }

        @Override
        public int hashCode() {
            return 31 * band + Arrays.hashCode(values);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BandKey)) {
                return false;
            }
            BandKey key = (BandKey) other;
            return band == key.band && Arrays.equals(values, key.values);
        }
    }
}
