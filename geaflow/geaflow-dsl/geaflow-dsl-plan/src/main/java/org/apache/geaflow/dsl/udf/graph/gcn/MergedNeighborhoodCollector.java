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

package org.apache.geaflow.dsl.udf.graph.gcn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.dsl.common.algo.AlgorithmModelRuntimeContext;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

public class MergedNeighborhoodCollector {

    public Row resolveMergedVertexValue(Object vertexId, RowVertex historyVertex,
                                        AlgorithmModelRuntimeContext<Object, Object> context) {
        Row dynamicValue = context.loadDynamicVertexValue(vertexId);
        if (dynamicValue != null) {
            return dynamicValue;
        }
        return historyVertex == null ? null : historyVertex.getValue();
    }

    public List<RowEdge> collectMergedEdges(Object vertexId,
                                            List<RowEdge> staticEdges,
                                            AlgorithmModelRuntimeContext<Object, Object> context,
                                            EdgeDirection direction,
                                            int fanout) {
        List<RowEdge> mergedEdges = new ArrayList<>(staticEdges.size()
            + context.loadDynamicEdges(vertexId, direction).size());
        mergedEdges.addAll(staticEdges);
        mergedEdges.addAll(context.loadDynamicEdges(vertexId, direction));
        if (fanout < 0 || mergedEdges.size() <= fanout) {
            return mergedEdges;
        }
        mergedEdges.sort(Comparator
            .comparingInt((RowEdge edge) -> sampleScore(vertexId, direction, edge))
            .thenComparing(this::edgeIdentity));
        return new ArrayList<>(mergedEdges.subList(0, fanout));
    }

    public Object getNeighborId(RowEdge edge, Object currentVertexId) {
        if (!Objects.equals(edge.getSrcId(), currentVertexId)) {
            return edge.getSrcId();
        }
        return edge.getTargetId();
    }

    private String edgeIdentity(RowEdge edge) {
        return String.valueOf(edge.getSrcId()) + "->" + edge.getTargetId() + "@"
            + edge.getLabel();
    }

    private int sampleScore(Object vertexId, EdgeDirection direction, RowEdge edge) {
        return Objects.hash(vertexId, direction, edgeIdentity(edge));
    }
}
