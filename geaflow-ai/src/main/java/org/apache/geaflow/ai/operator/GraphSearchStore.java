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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.common.config.Constants;
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
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

public class GraphSearchStore {

    public static final class ScoredGraphEntity {
        private final GraphEntity entity;
        private final float score;
        private final int rank;

        public ScoredGraphEntity(GraphEntity entity, float score, int rank) {
            this.entity = entity;
            this.score = score;
            this.rank = rank;
        }

        public GraphEntity getEntity() {
            return entity;
        }

        public float getScore() {
            return score;
        }

        public int getRank() {
            return rank;
        }
    }

    private SearchStore store;
    private long entityNum = 0L;

    public GraphSearchStore() {
        this.store = new SearchStore();
    }

    public boolean indexVertex(GraphVertex graphVertex, List<IVector> indexVectors) {
        Map<String, String> kv = new HashMap<>();
        Vertex vertex = graphVertex.getVertex();
        kv.put(SearchConstants.ID, vertex.getId());
        kv.put(SearchConstants.LABEL, vertex.getLabel());
        List<String> contents = new ArrayList<>(indexVectors.size());
        for (IVector v : indexVectors) {
            contents.add(v.toString());
        }
        String content = String.join(SearchConstants.DELIMITER, contents);
        kv.put(SearchConstants.CONTENT, content);

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
        kv.put(SearchConstants.SRC, edge.getSrcId());
        kv.put(SearchConstants.DST, edge.getDstId());
        kv.put(SearchConstants.LABEL, edge.getLabel());
        List<String> contents = new ArrayList<>(indexVectors.size());
        for (IVector v : indexVectors) {
            contents.add(v.toString());
        }
        String content = String.join(SearchConstants.DELIMITER, contents);
        kv.put(SearchConstants.CONTENT, content);
        try {
            store.addDoc(kv);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot index vertex to search store", e);
        }
        addItem();
        return true;
    }

    public List<GraphEntity> search(String key1, GraphAccessor graphAccessor) {
        List<ScoredGraphEntity> scored = searchScored(key1, graphAccessor,
            Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN, Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN,
            Long.MAX_VALUE);
        List<GraphEntity> result = new ArrayList<>(scored.size());
        for (ScoredGraphEntity hit : scored) {
            result.add(hit.getEntity());
        }
        return result;
    }

    public List<ScoredGraphEntity> searchScored(String key1, GraphAccessor graphAccessor,
                                                int topK, int maxCandidates,
                                                long deadlineNanos) {
        try {
            String query = SearchUtils.formatQuery(key1);
            TopDocs docs = store.searchDoc(SearchConstants.CONTENT, query,
                Math.max(topK, maxCandidates));
            ScoreDoc[] scoreDocArray = docs.scoreDocs;
            Set<String> vertexLabels = graphAccessor.getGraphSchema().getVertexSchemaList()
                    .stream().map(VertexSchema::getLabel).collect(Collectors.toSet());
            Set<String> edgeLabels = graphAccessor.getGraphSchema().getEdgeSchemaList()
                    .stream().map(EdgeSchema::getLabel).collect(Collectors.toSet());
            Map<String, ScoredGraphEntity> result = new LinkedHashMap<>();
            int candidateCount = 0;
            for (ScoreDoc scoreDoc : scoreDocArray) {
                if (candidateCount >= maxCandidates || result.size() >= topK
                    || System.nanoTime() > deadlineNanos) {
                    break;
                }
                candidateCount++;
                int docId = scoreDoc.doc;
                Document document = store.getDoc(docId);
                String label = document.get(SearchConstants.LABEL);
                if (vertexLabels.contains(label)) {
                    String id = document.get(SearchConstants.ID);
                    GraphVertex graphVertex = graphAccessor.getVertex(label, id);
                    if (graphVertex != null) {
                        putHighest(result, graphVertex, scoreDoc.score);
                    }
                } else if (edgeLabels.contains(label)) {
                    String src = document.get(SearchConstants.SRC);
                    String dst = document.get(SearchConstants.DST);
                    List<GraphEdge> graphEdge = graphAccessor.getEdge(label, src, dst);
                    if (graphEdge != null) {
                        for (GraphEdge edge : graphEdge) {
                            putHighest(result, edge, scoreDoc.score);
                        }
                    }
                }
            }
            List<ScoredGraphEntity> ordered = new ArrayList<>(result.values());
            ordered.sort((left, right) -> Float.compare(right.getScore(), left.getScore()));
            List<ScoredGraphEntity> limited = new ArrayList<>();
            for (ScoredGraphEntity hit : ordered) {
                if (limited.size() >= topK) {
                    break;
                }
                limited.add(new ScoredGraphEntity(hit.getEntity(), hit.getScore(), limited.size() + 1));
            }
            return limited;
        } catch (IndexNotFoundException notFoundException) {
            return new ArrayList<>();
        } catch (Exception e) {
            throw new RuntimeException("Cannot read search store", e);
        }
    }

    private static void putHighest(Map<String, ScoredGraphEntity> result, GraphEntity entity,
                                   float score) {
        String key;
        if (entity instanceof GraphVertex) {
            GraphVertex vertex = (GraphVertex) entity;
            key = "V:" + vertex.getVertex().getLabel() + ":" + vertex.getVertex().getId();
        } else {
            GraphEdge edge = (GraphEdge) entity;
            key = "E:" + edge.getEdge().getLabel() + ":" + edge.getEdge().getSrcId()
                + ":" + edge.getEdge().getDstId();
        }
        ScoredGraphEntity previous = result.get(key);
        if (previous == null || score > previous.getScore()) {
            result.put(key, new ScoredGraphEntity(entity, score, 0));
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

    public void finishWriting() {
        try {
            store.finishWriting();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot finish search store writes", e);
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
