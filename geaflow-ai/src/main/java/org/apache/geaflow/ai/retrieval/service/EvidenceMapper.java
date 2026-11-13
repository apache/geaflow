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

package org.apache.geaflow.ai.retrieval.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.operator.GraphSearchStore.ScoredGraphEntity;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;

/** Maps graph search hits to stable API evidence records. */
public final class EvidenceMapper {

    public List<Evidence> map(List<ScoredGraphEntity> hits) {
        Map<String, Evidence> unique = new LinkedHashMap<>();
        for (ScoredGraphEntity hit : hits == null ? Collections.<ScoredGraphEntity>emptyList() : hits) {
            GraphEntity entity = hit.getEntity();
            String identity = identity(entity);
            Map<String, ChannelScore> scores = new LinkedHashMap<>();
            scores.put("keyword", new ChannelScore("keyword", hit.getScore(),
                null, hit.getRank()));
            Evidence evidence = new Evidence(identity, EvidenceKind.ENTITY, entity.toString(),
                Collections.emptyList(), Collections.singletonList(toRef(entity)),
                Collections.emptyList(), Collections.emptyList(), scores,
                null, (double) hit.getScore(), hit.getRank());
            unique.putIfAbsent(identity, evidence);
        }
        return new ArrayList<>(unique.values());
    }

    private static String identity(GraphEntity entity) {
        if (entity instanceof GraphVertex) {
            GraphVertex vertex = (GraphVertex) entity;
            return "VERTEX:" + vertex.getVertex().getLabel() + ":" + vertex.getVertex().getId();
        }
        GraphEdge edge = (GraphEdge) entity;
        return "EDGE:" + edge.getEdge().getLabel() + ":" + edge.getEdge().getSrcId()
            + ":" + edge.getEdge().getDstId();
    }

    private static EntityRef toRef(GraphEntity entity) {
        if (entity instanceof GraphVertex) {
            GraphVertex vertex = (GraphVertex) entity;
            String id = vertex.getVertex().getId();
            return new EntityRef(id, id, vertex.getVertex().getLabel());
        }
        GraphEdge edge = (GraphEdge) entity;
        String id = edge.getEdge().getSrcId() + "->" + edge.getEdge().getDstId();
        return new EntityRef(id, id, edge.getEdge().getLabel());
    }
}
