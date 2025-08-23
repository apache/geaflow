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

package org.apache.geaflow.dsl.runtime.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.rel.GraphMatch;
import org.apache.geaflow.dsl.rel.ParameterizedRelNode;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.RexParameterRef;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RDataView;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicParameterizedRelNode extends ParameterizedRelNode implements PhysicRelNode<RDataView> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicParameterizedRelNode.class);

    public PhysicParameterizedRelNode(RelOptCluster cluster,
                                      RelTraitSet traitSet,
                                      RelNode parameter, RelNode query) {
        super(cluster, traitSet, parameter, query);
    }

    @Override
    public PhysicParameterizedRelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;
        return new PhysicParameterizedRelNode(getCluster(), traitSet, inputs.get(0), inputs.get(1));
    }

    @SuppressWarnings("unchecked")
    @Override
    public RDataView translate(QueryContext context) {
        RuntimeTable requestTable = ((PhysicRelNode<RuntimeTable>) getParameterNode()).translate(context);
        RelNode newQueryNode = isIdOnlyRequest();
        boolean idOnlyRequest = newQueryNode != getQueryNode();
        if (idOnlyRequest) {
            LOGGER.info("It is id only parameter request.");
        }
        RuntimeTable preRequestTable = context.setRequestTable(requestTable);
        boolean preIsIdOnlyRequest = context.setIdOnlyRequest(idOnlyRequest);
        RDataView dataView = ((PhysicRelNode<RDataView>) newQueryNode).translate(context);

        context.setRequestTable(preRequestTable);
        context.setIdOnlyRequest(preIsIdOnlyRequest);
        return dataView;
    }

    private RelNode isIdOnlyRequest() {
        Set<RexNode> idReferences = new HashSet<>();
        RelNode newQueryNode = isIdOnlyRequest(getQueryNode(), idReferences);
        // Only one id parameter has referred.
        if (idReferences.size() == 1 && newQueryNode != null) {
            return newQueryNode;
        }
        return getQueryNode();
    }

    private RelNode isIdOnlyRequest(RelNode node, Set<RexNode> idReferences) {
        if (node instanceof GraphMatch) {
            GraphMatch match = (GraphMatch) node;
            IMatchNode newPathPattern = (IMatchNode) isIdOnlyRequest(match.getPathPattern(), idReferences);
            if (newPathPattern == null) {
                return null;
            }
            return match.copy(newPathPattern);
        }
        RelNode newNode = node;
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());

        if (node instanceof MatchFilter
            && ((MatchFilter) node).getInput() instanceof VertexMatch
            && ((MatchFilter) node).getInput().getInputs().isEmpty()) {
            MatchFilter filter = (MatchFilter) node;
            VertexRecordType vertexRecordType = (VertexRecordType) ((VertexMatch) filter.getInput()).getNodeType();
            RexNode conditionRemoveId = GQLRexUtil.removeIdCondition(filter.getCondition(), vertexRecordType);

            Set<RexNode> ids = GQLRexUtil.findVertexIds(filter.getCondition(), vertexRecordType);
            idReferences.addAll(ids);
            // It contains parameter reference except the id request.
            boolean isIdOnlyRef = conditionRemoveId == null || !GQLRexUtil.contain(conditionRemoveId,
                RexParameterRef.class);
            VertexMatch vertexMatch = (VertexMatch) filter.getInput();
            // push filter to vertex-match.
            newInputs.add(vertexMatch.copy(filter.getCondition()));

            if (isIdOnlyRef) {
                if (conditionRemoveId != null) {
                    newNode = filter.copy(filter.getTraitSet(), filter.getInput(), conditionRemoveId);
                } else { // remove current filter.
                    return newInputs.get(0);
                }
            } else {
                return null;
            }
        } else {
            boolean containParameterRef =
                !GQLRexUtil.collect(node, rexNode -> rexNode instanceof RexParameterRef).isEmpty();
            if (containParameterRef) {
                return null;
            }
            for (RelNode input : node.getInputs()) {
                RelNode newInput = isIdOnlyRequest(input, idReferences);
                if (newInput == null) {
                    return null;
                }
                newInputs.add(newInput);
            }
        }
        return newNode.copy(node.getTraitSet(), newInputs);
    }

    @Override
    public String showSQL() {
        return null;
    }
}
