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
package org.apache.geaflow.ai.retrieval.execution;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.geaflow.ai.retrieval.channel.graph.EntityAnchorResolver;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;

/** Shared lexical statistics used to build adaptive routing features. */
public final class LexicalStatistics {
    private static final Pattern TOKEN = Pattern.compile("[\\p{L}\\p{N}]+",
        Pattern.UNICODE_CHARACTER_CLASS);

    private LexicalStatistics() {
    }

    /** Builds a document-frequency IDF index, counting each token at most once per chunk. */
    public static Map<String, Double> buildIdfIndex(List<TextChunk> chunks) {
        int documentCount = chunks == null ? 0 : chunks.size();
        Map<String, Integer> frequencies = new HashMap<>();
        if (chunks != null) {
            for (TextChunk chunk : chunks) {
                Matcher matcher = TOKEN.matcher(EntityAnchorResolver.normalize(chunk.getText()));
                Set<String> present = new HashSet<>();
                while (matcher.find()) {
                    present.add(matcher.group());
                }
                for (String token : present) {
                    frequencies.put(token, frequencies.getOrDefault(token, 0) + 1);
                }
            }
        }
        Map<String, Double> index = new HashMap<>();
        for (Map.Entry<String, Integer> entry : frequencies.entrySet()) {
            index.put(entry.getKey(), Math.log(1.0D + (documentCount + 1.0D)
                / (entry.getValue() + 1.0D)));
        }
        return Collections.unmodifiableMap(index);
    }

    /** Returns the number of normalized lexical tokens in a query. */
    public static int queryLength(String query) {
        Matcher matcher = TOKEN.matcher(EntityAnchorResolver.normalize(query));
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    /** Returns the mean IDF of unique normalized query tokens. */
    public static double averageIdf(String query, Map<String, Double> idfByToken) {
        Set<String> queryTokens = new TreeSet<>();
        Matcher matcher = TOKEN.matcher(EntityAnchorResolver.normalize(query));
        while (matcher.find()) {
            queryTokens.add(matcher.group());
        }
        if (queryTokens.isEmpty() || idfByToken == null || idfByToken.isEmpty()) {
            return 0.0D;
        }
        double total = 0.0D;
        for (String token : queryTokens) {
            total += idfByToken.getOrDefault(token, 1.0D);
        }
        return total / queryTokens.size();
    }
}
