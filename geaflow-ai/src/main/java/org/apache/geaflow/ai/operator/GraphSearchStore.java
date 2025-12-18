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

package org.apache.geaflow.ai.operator;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

public class GraphSearchStore {

    private SearchStore store;
    private long entityNum = 0L;

    public GraphSearchStore() {
        this.store = new SearchStore();
    }

    public boolean indexVertex(GraphVertex graphVertex, List<IVector> indexVectors) {
        Map<String, String> kv = new HashMap<>();
        Vertex vertex = graphVertex.getVertex();
        kv.put("id", vertex.getId());
        kv.put("label", vertex.getLabel());
        List<String> contents = new ArrayList<>(indexVectors.size());
        for (IVector v : indexVectors) {
            contents.add(v.toString());
        }
        String content = String.join("  ", contents);
        kv.put("content", content);

        try {
            store.addDoc(kv);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot index vertex to search store", e);
        }
        addItem();
        return true;
    }

    public boolean indexEdge(GraphEdge graphEdge, List<IVector> indexVectors) {
        Map<String, String> kv = new HashMap<>();
        Edge edge = graphEdge.getEdge();
        kv.put("src", edge.getSrcId());
        kv.put("dst", edge.getDstId());
        kv.put("label", edge.getLabel());
        List<String> contents = new ArrayList<>(indexVectors.size());
        for (IVector v : indexVectors) {
            contents.add(v.toString());
        }
        String content = String.join("  ", contents);
        kv.put("content", content);
        try {
            store.addDoc(kv);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot index vertex to search store", e);
        }
        addItem();
        return true;
    }

    public List<GraphEntity> search(String key1, GraphAccessor graphAccessor) {
        try {
            String query = SearchUtils.formatQuery(key1);
            TopDocs docs = store.searchDoc("content", query);
            ScoreDoc[] scoreDocArray = docs.scoreDocs;
            Set<String> vertexLabels = graphAccessor.getGraphSchema().getVertexSchemaList()
                    .stream().map(VertexSchema::getLabel).collect(Collectors.toSet());
            Set<String> edgeLabels = graphAccessor.getGraphSchema().getEdgeSchemaList()
                    .stream().map(EdgeSchema::getLabel).collect(Collectors.toSet());
            List<GraphEntity> result = new ArrayList<>();
            for (ScoreDoc scoreDoc : scoreDocArray) {
                int docId = scoreDoc.doc;
                Document document = store.getDoc(docId);
                String label = document.get(SearchConstants.LABEL);
                if (vertexLabels.contains(label)) {
                    String id = document.get(SearchConstants.ID);
                    result.add(graphAccessor.getVertex(label, id));
                } else if (edgeLabels.contains(label)) {
                    String src = document.get(SearchConstants.SRC);
                    String dst = document.get(SearchConstants.DST);
                    result.add(graphAccessor.getEdge(label, src, dst));
                }
            }
            return result;
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read search store", e);
        }
    }

    private void addItem() {
        entityNum++;
    }

    public void close() {
        try {
            store.close();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot close search store", e);
        }
    }

    public Directory getDirectory() {
        return store.getDirectory();
    }

    public Analyzer getAnalyzer() {
        return store.getAnalyzer();
    }

    public IndexWriterConfig getConfig() {
        return store.getConfig();
    }


}
