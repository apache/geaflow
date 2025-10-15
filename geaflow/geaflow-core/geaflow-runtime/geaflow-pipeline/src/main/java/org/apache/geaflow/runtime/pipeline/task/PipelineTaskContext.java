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

package org.apache.geaflow.runtime.pipeline.task;

import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.pdata.graph.view.IncGraphView;
import org.apache.geaflow.pdata.graph.window.WindowStreamGraph;
import org.apache.geaflow.pdata.stream.window.WindowStreamSource;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.runtime.pipeline.PipelineContext;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;

public class PipelineTaskContext implements IPipelineTaskContext {

    private long pipelineTaskId;
    private PipelineContext pipelineContext;

    public PipelineTaskContext(long pipelineTaskId,
                               PipelineContext pipelineContext) {
        this.pipelineTaskId = pipelineTaskId;
        this.pipelineContext = pipelineContext;
    }

    @Override
    public long getId() {
        return this.pipelineTaskId;
    }

    @Override
    public Configuration getConfig() {
        return pipelineContext.getConfig();
    }

    @Override
    public <T> PWindowSource<T> buildSource(SourceFunction<T> sourceFunction, IWindow<T> window) {
        return new WindowStreamSource<>(pipelineContext, sourceFunction, window);
    }

    @Override
    public <K, VV, EV> PGraphView<K, VV, EV> getGraphView(String viewName) {
        IViewDesc viewDesc = pipelineContext.getViewDesc(viewName);
        return new IncGraphView<>(pipelineContext, viewDesc);
    }

    @Override
    public <K, VV, EV> PGraphView<K, VV, EV> createGraphView(IViewDesc viewDesc) {
        return new IncGraphView<>(pipelineContext, viewDesc);
    }

    @Override
    public <K, VV, EV> PGraphWindow<K, VV, EV> buildWindowStreamGraph(PWindowStream<IVertex<K, VV>> vertexWindowSteam,
                                                                      PWindowStream<IEdge<K, EV>> edgeWindowStream,
                                                                      GraphViewDesc graphViewDesc) {
        return new WindowStreamGraph(graphViewDesc, pipelineContext, vertexWindowSteam, edgeWindowStream);
    }

}
