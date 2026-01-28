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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.ai.GraphMemoryServer;
import org.apache.geaflow.ai.common.ErrorCode;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.*;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeywordRelationFunction implements ConsolidateFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeywordRelationFunction.class);

    @Override
    public void eval(GraphAccessor graphAccessor, MutableGraph mutableGraph) {
        EntityAttributeIndexStore indexStore = new EntityAttributeIndexStore();
        indexStore.initStore(new SubgraphSemanticPromptFunction(graphAccessor));
        LOGGER.info("Success to init EntityAttributeIndexStore.");
        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);
        LOGGER.info("Success to init GraphMemoryServer.");

        if (null == mutableGraph.getSchema().getSchema(
            Constants.CONSOLIDATE_KEYWORD_RELATION_LABEL)) {
            int code = mutableGraph.addEdgeSchema(new EdgeSchema(
                Constants.CONSOLIDATE_KEYWORD_RELATION_LABEL,
                Constants.PREFIX_SRC_ID, Constants.PREFIX_DST_ID,
                Collections.singletonList(Constants.PREFIX_COMMON_KEYWORDS)
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
                if (existEdge.getLabel().equals(Constants.CONSOLIDATE_KEYWORD_RELATION_LABEL)) {
                    existRelation = true;
                    break;
                }
            }
            if (existRelation) {
                continue;
            }
            String sessionId = server.createSession();
            VectorSearch search = new VectorSearch(null, sessionId);
            search.addVector(new KeywordVector(vertex.toString()));
            sessionId = server.search(search);
            List<GraphEntity> results = server.getSessionEntities(sessionId);
            for (GraphEntity relateEntity : results) {
                if (relateEntity instanceof GraphVertex) {
                    String srcId = vertex.getVertex().getId();
                    String dstId = ((GraphVertex) relateEntity).getVertex().getId();
                    mutableGraph.addEdge(new Edge(
                        Constants.CONSOLIDATE_KEYWORD_RELATION_LABEL,
                        srcId, dstId,
                        Collections.singletonList(Constants.PREFIX_COMMON_KEYWORDS)
                    ));
                }
            }

            cnt++;
            LOGGER.info("Process vertex num: {}", cnt);
        }

    }
}
