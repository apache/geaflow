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

package org.apache.geaflow.ai.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.operator.SearchStore;
import org.apache.geaflow.ai.operator.SearchUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Golden tests for keyword tokenization and query formatting.
 *
 * <p>Both halves below pin behavior that has no test today and is easy to break without noticing:
 * a Lucene version bump changes how text is split, and any edit to {@link SearchUtils} changes
 * what actually reaches the query parser. When one of these tests fails the right move is to
 * re-read the diff against the recorded golden files — an intentional change updates the golden
 * with a note, an accidental one gets fixed.
 *
 * <p>The golden files live in {@code src/test/resources/index/keyword/} and were generated against
 * {@code lucene-core} 8.11.2, the version {@code geaflow-ai} pins.
 */
public class KeywordTokenizationGoldenTest {

    private static final String TOKENIZATION_GOLDEN = "/index/keyword/tokenization_golden.txt";
    private static final String QUERY_FORMAT_GOLDEN = "/index/keyword/query_format_golden.txt";

    /**
     * The analyzer is taken from {@link SearchStore}, the same instance the index and the query
     * parser share in production, so the golden records the terms documents are actually indexed
     * under. Recorded facts worth knowing before "fixing" a failure:
     *
     * <ul>
     *   <li>English stop words are <b>not</b> dropped. {@code StandardAnalyzer()} in lucene-core
     *       8.11.2 is built with an empty stop-word set, so {@code the} and {@code of} are real,
     *       searchable terms (see {@code KeywordIndexLifecycleGoldenTest} for what that means at
     *       query time).</li>
     *   <li>CJK text yields one single-character term per character.</li>
     *   <li>Hyphens, slashes and {@code ?} split a word; intra-word dots and {@code @} do not
     *       ({@code v2.0}, {@code a.b} stay whole).</li>
     * </ul>
     */
    @Test
    public void tokenizationMatchesGolden() throws IOException {
        SearchStore store = new SearchStore();
        List<String> failures = new ArrayList<>();
        for (String[] row : goldenRows(TOKENIZATION_GOLDEN)) {
            String input = row[0];
            String expected = row[1];
            String actual = String.join("|", tokenize(store, input));
            if (!expected.equals(actual)) {
                failures.add(String.format("input [%s]: expected [%s] but tokenized to [%s]",
                    input, expected, actual));
            }
        }
        Assertions.assertTrue(failures.isEmpty(),
            "keyword tokenization changed; review against " + TOKENIZATION_GOLDEN + ":\n"
                + String.join("\n", failures));
    }

    /**
     * {@link SearchUtils#formatQuery} is the only pre-processing a query string goes through
     * before the Lucene query parser. The golden pins its two rules: excluded characters become
     * spaces, then the lower-case substring {@code http} is erased — a plain substring replace,
     * so {@code https://x} loses its scheme down to {@code s}.
     */
    @Test
    public void queryFormattingMatchesGolden() throws IOException {
        List<String> failures = new ArrayList<>();
        for (String[] row : goldenRows(QUERY_FORMAT_GOLDEN)) {
            String input = row[0];
            String expected = row[1];
            String actual = SearchUtils.formatQuery(input);
            if (!expected.equals(actual)) {
                failures.add(String.format("input [%s]: expected [%s] but formatted to [%s]",
                    input, expected, actual));
            }
        }
        Assertions.assertTrue(failures.isEmpty(),
            "query formatting changed; review against " + QUERY_FORMAT_GOLDEN + ":\n"
                + String.join("\n", failures));
    }

    /** Whitespace-only input is the degenerate case of the empty input: no terms either way. */
    @Test
    public void whitespaceOnlyInputYieldsNoTerms() throws IOException {
        SearchStore store = new SearchStore();
        Assertions.assertTrue(tokenize(store, "   ").isEmpty(),
            "a whitespace-only value contributes no terms");
    }

    private List<String> tokenize(SearchStore store, String input) throws IOException {
        List<String> terms = new ArrayList<>();
        try (TokenStream stream = store.getAnalyzer().tokenStream("content", input)) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                terms.add(term.toString());
            }
            stream.end();
        }
        return terms;
    }

    /** Parses "<input> => <output>" rows, skipping blank lines and '#' comments. */
    private static List<String[]> goldenRows(String resource) throws IOException {
        InputStream in = KeywordTokenizationGoldenTest.class.getResourceAsStream(resource);
        Assertions.assertNotNull(in, "missing golden resource " + resource);
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(in, StandardCharsets.UTF_8))) {
            List<String[]> rows = new ArrayList<>();
            for (String line : reader.lines()
                .map(l -> l.replace("\r", ""))
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .collect(Collectors.toList())) {
                String[] parts = line.split(" => ", 2);
                Assertions.assertEquals(2, parts.length,
                    "malformed golden row (expected '<input> => <output>'): " + line);
                rows.add(parts);
            }
            return rows;
        }
    }
}
