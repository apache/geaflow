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

package org.apache.geaflow.ai.consolidate.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.ai.common.ErrorCode;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.*;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.common.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddingRelationFunction implements ConsolidateFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingRelationFunction.class);

    @Override
    public void eval(GraphAccessor graphAccessor, MutableGraph mutableGraph,
                     List<IndexStore> indexStores) {
        EmbeddingIndexStore embeddingIndexStore = null;
        for (IndexStore indexStore : indexStores) {
            if (indexStore instanceof EmbeddingIndexStore) {
                embeddingIndexStore = (EmbeddingIndexStore) indexStore;
            }
        }
        if (embeddingIndexStore == null) {
            return;
        }
        if (null == mutableGraph.getSchema().getSchema(
            Constants.CONSOLIDATE_EMBEDDING_RELATION_LABEL)) {
            int code = mutableGraph.addEdgeSchema(new EdgeSchema(
                Constants.CONSOLIDATE_EMBEDDING_RELATION_LABEL,
                Constants.PREFIX_SRC_ID, Constants.PREFIX_DST_ID,
                Collections.singletonList(Constants.PREFIX_EMBEDDING_KEYWORDS)
            ));
            if (code != ErrorCode.SUCCESS) {
                return;
            }
        }

        Long cnt = 0L;
        Iterator<GraphVertex> vertexIterator = graphAccessor.scanVertex();
        while (vertexIterator.hasNext()) {
            GraphVertex vertex = vertexIterator.next();
            boolean existRelation = false;
            Iterator<GraphEdge> neighborIterator = graphAccessor.scanEdge(vertex);
            while (neighborIterator.hasNext()) {
                GraphEdge existEdge = neighborIterator.next();
                if (existEdge.getLabel().equals(Constants.CONSOLIDATE_EMBEDDING_RELATION_LABEL)) {
                    existRelation = true;
                    break;
                }
            }
            if (existRelation) {
                continue;
            }
            //Traverse other vertices, sort them by vector cosine similarity in ascending order,
            // and insert the relationship edges in a binary fashion.
            List<IVector> embedding = embeddingIndexStore.getEntityIndex(vertex);
            Iterator<GraphVertex> relationIterator = graphAccessor.scanVertex();
            List<GraphEntity> results = new ArrayList<>();

            List<Tuple<GraphVertex, Double>> similarityList = new ArrayList<>();
            while (relationIterator.hasNext()) {
                GraphVertex relateVertex = relationIterator.next();
                if (vertex.equals(relateVertex)) {
                    continue;
                }
                List<IVector> relateVertexEmbedding =
                    embeddingIndexStore.getEntityIndex(relateVertex);
                double relRatio = calRel(embedding, relateVertexEmbedding);
                similarityList.add(new Tuple<>(relateVertex, relRatio));
            }
            similarityList.sort((a, b) -> Double.compare(b.getF1(), a.getF1()));

            int currentIndex = 0;
            int step = 1;

            while (currentIndex < similarityList.size()) {
                results.add(similarityList.get(currentIndex).getF0());
                step *= 2;
                currentIndex = step - 1;
            }

            for (GraphEntity relateEntity : results) {
                if (relateEntity instanceof GraphVertex) {
                    String srcId = vertex.getVertex().getId();
                    String dstId = ((GraphVertex) relateEntity).getVertex().getId();
                    mutableGraph.addEdge(new Edge(
                        Constants.CONSOLIDATE_EMBEDDING_RELATION_LABEL,
                        srcId, dstId,
                        Collections.singletonList(Constants.PREFIX_EMBEDDING_KEYWORDS)
                    ));
                }
            }

            cnt++;
            LOGGER.info("Process vertex num: {}", cnt);
        }
    }

    private static double calRel(List<IVector> vecAList, List<IVector> vecBList) {
        double max = 0.0;
        for (IVector vec : vecAList) {
            if (vec instanceof EmbeddingVector) {
                for (IVector vecB : vecBList) {
                    max = Math.max(max, vec.match(vecB));
                }
            }
        }
        return max;
    }
}
