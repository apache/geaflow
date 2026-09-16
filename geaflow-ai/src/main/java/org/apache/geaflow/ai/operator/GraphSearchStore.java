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
import org.apache.geaflow.ai.common.model.ModelUtils;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

public class GraphSearchStore {

    private final SearchStore store;

    /** Label sets of the schema last searched against, cached to avoid rebuilding them per query. */
    private volatile GraphSchema cachedSchema;
    private volatile Set<String> cachedVertexLabels;
    private volatile Set<String> cachedEdgeLabels;

    public GraphSearchStore() {
        this.store = new SearchStore();
    }

    /**
     * Number of live documents in the index. Cheaper and more trustworthy than tracking a counter
     * alongside Lucene, which would have to mirror update and delete semantics.
     */
    public int getDocNum() {
        try {
            return store.numDocs();
        } catch (IndexNotFoundException notFoundException) {
            return 0;
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read search store", e);
        }
    }

    /**
     * Makes previously indexed entities searchable without discarding the index.
     */
    public void refresh() {
        try {
            store.refresh();
        } catch (IndexNotFoundException notFoundException) {
            // Nothing has been indexed yet, there is nothing to make visible.
        } catch (Throwable e) {
            throw new RuntimeException("Cannot refresh search store", e);
        }
    }

    public boolean indexVertex(GraphVertex graphVertex, List<IVector> indexVectors) {
        return writeDoc(vertexDoc(graphVertex, indexVectors), false);
    }

    /**
     * Adds or replaces a vertex document in place, keyed by
     * {@link ModelUtils#getGraphEntityKey}. Idempotent: calling it twice for the same vertex leaves
     * a single document.
     */
    public boolean upsertVertex(GraphVertex graphVertex, List<IVector> indexVectors) {
        return writeDoc(vertexDoc(graphVertex, indexVectors), true);
    }

    public boolean indexEdge(GraphEdge graphEdge, List<IVector> indexVectors) {
        return writeDoc(edgeDoc(graphEdge, indexVectors), false);
    }

    public boolean upsertEdge(GraphEdge graphEdge, List<IVector> indexVectors) {
        return writeDoc(edgeDoc(graphEdge, indexVectors), true);
    }

    /**
     * Marks the entity's document as deleted. Lucene flips a bit in a per segment bitset, so the
     * cost is independent of index size and no rebuild is needed.
     */
    public boolean removeEntity(GraphEntity entity) {
        if (entity == null) {
            return false;
        }
        try {
            store.deleteDoc(SearchConstants.KEY, ModelUtils.getGraphEntityKey(entity));
        } catch (Throwable e) {
            throw new RuntimeException("Cannot remove entity from search store", e);
        }
        return true;
    }

    private Map<String, String> vertexDoc(GraphVertex graphVertex, List<IVector> indexVectors) {
        Map<String, String> kv = new HashMap<>();
        Vertex vertex = graphVertex.getVertex();
        kv.put(SearchConstants.KEY, ModelUtils.getGraphEntityKey(graphVertex));
        kv.put(SearchConstants.ID, vertex.getId());
        kv.put(SearchConstants.LABEL, vertex.getLabel());
        kv.put(SearchConstants.CONTENT, joinVectors(indexVectors));
        return kv;
    }

    private Map<String, String> edgeDoc(GraphEdge graphEdge, List<IVector> indexVectors) {
        Map<String, String> kv = new HashMap<>();
        Edge edge = graphEdge.getEdge();
        kv.put(SearchConstants.KEY, ModelUtils.getGraphEntityKey(graphEdge));
        kv.put(SearchConstants.SRC, edge.getSrcId());
        kv.put(SearchConstants.DST, edge.getDstId());
        kv.put(SearchConstants.LABEL, edge.getLabel());
        kv.put(SearchConstants.CONTENT, joinVectors(indexVectors));
        return kv;
    }

    private String joinVectors(List<IVector> indexVectors) {
        List<String> contents = new ArrayList<>(indexVectors.size());
        for (IVector v : indexVectors) {
            contents.add(v.toString());
        }
        return String.join(SearchConstants.DELIMITER, contents);
    }

    private boolean writeDoc(Map<String, String> kv, boolean upsert) {
        try {
            if (upsert) {
                store.updateDoc(SearchConstants.KEY, kv.get(SearchConstants.KEY), kv);
            } else {
                store.addDoc(kv, SearchConstants.KEY);
            }
        } catch (Throwable e) {
            throw new RuntimeException("Cannot index entity to search store", e);
        }
        return true;
    }

    public List<GraphEntity> search(String key1, GraphAccessor graphAccessor) {
        try {
            String query = SearchUtils.formatQuery(key1);
            TopDocs docs = store.searchDoc(SearchConstants.CONTENT, query);
            ScoreDoc[] scoreDocArray = docs.scoreDocs;
            GraphSchema schema = graphAccessor.getGraphSchema();
            // Schemas are only ever appended to, so the object plus the two list sizes identify the
            // cached label sets. Getting this wrong would silently drop hits, so keep it strict.
            if (schema != cachedSchema
                    || cachedVertexLabels.size() != schema.getVertexSchemaList().size()
                    || cachedEdgeLabels.size() != schema.getEdgeSchemaList().size()) {
                cachedVertexLabels = schema.getVertexSchemaList().stream()
                        .map(VertexSchema::getLabel).collect(Collectors.toSet());
                cachedEdgeLabels = schema.getEdgeSchemaList().stream()
                        .map(EdgeSchema::getLabel).collect(Collectors.toSet());
                cachedSchema = schema;
            }
            Set<String> vertexLabels = cachedVertexLabels;
            Set<String> edgeLabels = cachedEdgeLabels;
            List<GraphEntity> result = new ArrayList<>();
            for (ScoreDoc scoreDoc : scoreDocArray) {
                int docId = scoreDoc.doc;
                Document document = store.getDoc(docId);
                String label = document.get(SearchConstants.LABEL);
                if (vertexLabels.contains(label)) {
                    String id = document.get(SearchConstants.ID);
                    GraphVertex graphVertex = graphAccessor.getVertex(label, id);
                    if (graphVertex != null) {
                        result.add(graphVertex);
                    }
                } else if (edgeLabels.contains(label)) {
                    String src = document.get(SearchConstants.SRC);
                    String dst = document.get(SearchConstants.DST);
                    List<GraphEdge> graphEdge = graphAccessor.getEdge(label, src, dst);
                    if (graphEdge != null) {
                        result.addAll(graphEdge);
                    }
                }
            }
            return result;
        } catch (IndexNotFoundException notFoundException) {
            return new ArrayList<>();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read search store", e);
        }
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
}
