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
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.operator.GraphSearchStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Golden test for the wire format {@link GraphSearchStore} writes into the Lucene index.
 *
 * <p>An entity is not indexed under its values directly. Each {@code IVector} is stringified and
 * the results are joined into one {@code content} field, so what actually lands in the index for a
 * {@link KeywordVector} is the wrapper text {@code KeywordVector{vec=[a, b]}}. That string is the
 * contract between indexing and retrieval — every query is matched against tokens produced from
 * it — yet nothing pinned it before.
 *
 * <p>The wrapper contributes the tokens {@code keywordvector} and {@code vec} to <b>every</b>
 * document. {@link KeywordIndexLifecycleGoldenTest} shows what that costs at query time; this test
 * only locks the format so that any change (for example a future PR replacing {@code toString()}
 * with a dedicated query representation) turns into a visible diff here instead of a silent
 * recall shift.
 */
public class KeywordIndexSerializationGoldenTest {

    private static final String SERIALIZATION_GOLDEN = "/index/keyword/serialized_docs_golden.txt";

    /**
     * Indexes one vertex, one edge and one vertex carrying two vectors, closes the store the way
     * the production path does, then reads every document back field by field and compares against
     * the golden file. Rendering is {@code <kind> field=[value] ...} with the production field
     * order (vertices id/label/content, edges src/dst/label/content).
     */
    @Test
    public void indexedDocumentsMatchGolden() throws IOException {
        GraphSearchStore store = new GraphSearchStore();
        store.indexVertex(new GraphVertex(
                new Vertex("chunk", "v1", Collections.singletonList("the alpaca grazes"))),
            Collections.singletonList(new KeywordVector("alpaca", "grazes")));
        store.indexEdge(new GraphEdge(
                new Edge("rel", "v1", "v2", Collections.singletonList("x"))),
            Collections.singletonList(new KeywordVector("master")));
        store.indexVertex(new GraphVertex(
                new Vertex("chunk", "v3", Collections.singletonList("two vectors"))),
            List.of(new KeywordVector("a"), new KeywordVector("b")));
        store.close();

        List<String> actual = new ArrayList<>();
        try (DirectoryReader reader = DirectoryReader.open(store.getDirectory())) {
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document doc = reader.document(i);
                if (doc.get("id") != null) {
                    actual.add(String.format("vertex id=[%s] label=[%s] content=[%s]",
                        doc.get("id"), doc.get("label"), doc.get("content")));
                } else {
                    actual.add(String.format("edge src=[%s] dst=[%s] label=[%s] content=[%s]",
                        doc.get("src"), doc.get("dst"), doc.get("label"), doc.get("content")));
                }
            }
        }

        Assertions.assertEquals(goldenLines(), actual,
            "the serialized form of indexed keyword vectors changed; review against "
                + SERIALIZATION_GOLDEN);
    }

    private static List<String> goldenLines() throws IOException {
        InputStream in = KeywordIndexSerializationGoldenTest.class
            .getResourceAsStream(SERIALIZATION_GOLDEN);
        Assertions.assertNotNull(in, "missing golden resource " + SERIALIZATION_GOLDEN);
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines()
                .map(line -> line.replace("\r", ""))
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .collect(java.util.stream.Collectors.toList());
        }
    }
}
