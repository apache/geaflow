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

package org.apache.geaflow.dsl.runtime;

import java.util.Set;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.runtime.util.SchemaUtil;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.pipeline.job.IPipelineJobContext;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;
import org.apache.geaflow.view.graph.PIncGraphView;

public class InsertGraphMaterialCallback implements QueryCallback {

    public static final QueryCallback INSTANCE = new InsertGraphMaterialCallback();

    private InsertGraphMaterialCallback() {

    }

    @Override
    public void onQueryFinish(QueryContext queryContext) {
        Set<String> nonMaterializedGraphs = queryContext.getNonMaterializedGraphs();
        for (String graphName : nonMaterializedGraphs) {
            PWindowStream<RowVertex> vertexStream = queryContext.getGraphVertexStream(graphName);
            PWindowStream<RowEdge> edgeStream = queryContext.getGraphEdgeStream(graphName);

            GeaFlowGraph graph = queryContext.getGraph(graphName);
            queryContext.updateVertexAndEdgeToGraph(graphName, graph, vertexStream, edgeStream);

            GraphViewDesc graphViewDesc = SchemaUtil.buildGraphViewDesc(graph, queryContext.getGlobalConf());
            IPipelineJobContext pipelineContext = queryContext.getEngineContext().getContext();
            PGraphView<Object, RowVertex, RowEdge> graphView = pipelineContext.createGraphView(graphViewDesc);
            PIncGraphView<Object, RowVertex, RowEdge> incGraphView =
                graphView.appendGraph((PWindowStream) vertexStream, (PWindowStream) edgeStream);

            incGraphView.materialize();
            queryContext.addMaterializedGraph(graphName);
        }
    }
}
