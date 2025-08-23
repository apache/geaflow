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

package org.apache.geaflow.plan.visualization;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.geaflow.common.visualization.console.ConsoleVisualizeVertex;
import org.apache.geaflow.common.visualization.console.JsonPlan;
import org.apache.geaflow.common.visualization.console.Predecessor;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.apache.geaflow.plan.graph.VertexType;

public class JsonPlanGraphVisualization {

    private final PipelineGraph plan;

    private final JsonPlan jsonPlan;

    public JsonPlanGraphVisualization(PipelineGraph plan) {
        this.plan = plan;
        this.jsonPlan = new JsonPlan();
        createJsonPlan(plan);
    }

    private void createJsonPlan(PipelineGraph plan) {
        List<PipelineVertex> pipelineVertices = new ArrayList<>(plan.getVertexMap().values());
        pipelineVertices.sort(Comparator.comparingInt(PipelineVertex::getVertexId));

        for (PipelineVertex vertex : pipelineVertices) {
            ConsoleVisualizeVertex v = new ConsoleVisualizeVertex();
            decorateNode(vertex, v);
            jsonPlan.vertices.put(v.id, v);
        }
    }

    public JsonPlan getJsonPlan() {
        return jsonPlan;
    }

    private void decorateNode(PipelineVertex jobVertex, ConsoleVisualizeVertex vertex) {
        vertex.setId(Integer.toString(jobVertex.getVertexId()));
        vertex.setVertexType(jobVertex.getType().name());
        vertex.setParallelism(jobVertex.getParallelism());
        vertex.setVertexMode(jobVertex.getVertexMode() == null ? null :
            jobVertex.getVertexMode().name());

        if (!vertex.getVertexType().equals(VertexType.source.name())) {
            for (PipelineEdge edge : plan.getVertexInputEdges().get(jobVertex.getVertexId())) {
                Predecessor predecessor = new Predecessor();
                predecessor.setId(Integer.toString(edge.getSrcId()));
                predecessor.setPartitionType(edge.getPartitionType().name());
                vertex.getParents().add(predecessor);
            }
        }
        AbstractOperator<?> operator = (AbstractOperator<?>) jobVertex.getOperator();
        if (operator.getNextOperators().size() > 0) {
            vertex.setInnerPlan(new JsonPlan());
            decorateInnerOperator(vertex.getInnerPlan(), operator, vertex, null);
        } else {
            vertex.setOperator(operator.getClass().getSimpleName());
            vertex.setOperatorName(getOperatorName(operator));
        }
    }

    private void decorateInnerOperator(JsonPlan innerPlan, AbstractOperator<?> operator,
                                       ConsoleVisualizeVertex outerVertex, String parentId) {
        ConsoleVisualizeVertex vertex = new ConsoleVisualizeVertex();
        vertex.setOperator(operator.getClass().getSimpleName());
        vertex.setOperatorName(getOperatorName(operator));

        vertex.setId(outerVertex.getId() + "-" + operator.getOpArgs().getOpId());
        vertex.setParallelism(outerVertex.getParallelism());
        innerPlan.vertices.put(vertex.getId(), vertex);
        if (parentId != null) {
            Predecessor predecessor = new Predecessor();
            predecessor.setId(parentId);
            vertex.getParents().add(predecessor);
        }
        for (Operator op : operator.getNextOperators()) {
            decorateInnerOperator(innerPlan, (AbstractOperator<?>) op, outerVertex, vertex.getId());
        }
    }

    private String getOperatorName(AbstractOperator<?> operator) {
        if (StringUtils.isNotBlank(operator.getOpArgs().getOpName())) {
            return operator.getOpArgs().getOpName();
        }
        return operator.getIdentify();
    }
}
