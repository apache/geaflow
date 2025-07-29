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

package org.apache.geaflow.operator.impl.graph.compute.statical;

import org.apache.geaflow.api.graph.base.algo.VertexCentricAlgo;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStaticGraphVertexCentricOp<K, VV, EV, M, FUNC extends VertexCentricAlgo<K, VV, EV, M>>
    extends AbstractGraphVertexCentricOp<K, VV, EV, M, FUNC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractStaticGraphVertexCentricOp.class);

    public AbstractStaticGraphVertexCentricOp(GraphViewDesc graphViewDesc, FUNC func) {
        super(graphViewDesc, func);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        if (enableDebug) {
            LOGGER.info("taskId:{} add vertex:{}", taskId, vertex);
        }
        this.graphState.staticGraph().V().add(vertex);
        this.opInputMeter.mark();
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        if (enableDebug) {
            LOGGER.info("taskId:{} add edge:{}", taskId, edge);
        }
        this.graphState.staticGraph().E().add(edge);
        this.opInputMeter.mark();
    }

    @Override
    protected GraphStateDescriptor<K, VV, EV> buildGraphStateDesc(String name) {
        GraphStateDescriptor<K, VV, EV> desc = super.buildGraphStateDesc(name);
        desc.withDataModel(graphViewDesc.isStatic() ? DataModel.STATIC_GRAPH : DataModel.DYNAMIC_GRAPH);
        if (graphViewDesc.getGraphMetaType() != null) {
            desc.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        }
        return desc;
    }

    public GraphViewDesc getGraphViewDesc() {
        return graphViewDesc;
    }
}
