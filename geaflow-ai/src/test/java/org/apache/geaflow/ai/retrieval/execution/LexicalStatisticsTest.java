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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.junit.jupiter.api.Test;

class LexicalStatisticsTest {

    @Test
    void idfCountsEachNormalizedTokenOncePerChunk() {
        Map<String, Double> idf = LexicalStatistics.buildIdfIndex(Arrays.asList(
            chunk("c1", "Apple apple banana"), chunk("c2", "banana carrot")));

        assertEquals(Math.log(2.5D), idf.get("apple"), 1.0e-12);
        assertEquals(Math.log(2.0D), idf.get("banana"), 1.0e-12);
        assertEquals(3, LexicalStatistics.queryLength("APPLE banana banana"));
        assertEquals((Math.log(2.5D) + Math.log(2.0D)) / 2.0D,
            LexicalStatistics.averageIdf("APPLE banana banana", idf), 1.0e-12);
        assertThrows(UnsupportedOperationException.class, () -> idf.put("other", 1.0D));
    }

    @Test
    void emptyQueryOrIndexHasZeroAverageIdf() {
        assertEquals(0.0D, LexicalStatistics.averageIdf(" ", Collections.emptyMap()));
        assertEquals(0.0D, LexicalStatistics.averageIdf("query", Collections.emptyMap()));
        assertEquals(Collections.emptyMap(), LexicalStatistics.buildIdfIndex(Collections.emptyList()));
    }

    private static TextChunk chunk(String id, String text) {
        return new TextChunk(id, "doc-" + id, 0, 0, text.length(), 1, text);
    }
}
