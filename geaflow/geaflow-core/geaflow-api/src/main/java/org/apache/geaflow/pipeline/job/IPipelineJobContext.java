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

package org.apache.geaflow.pipeline.job;

import java.io.Serializable;
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;

public interface IPipelineJobContext extends Serializable {

    /**
     * Returns pipeline id.
     */
    long getId();

    /**
     * Returns pipeline config.
     */
    Configuration getConfig();

    /**
     * Build window source with source function and window.
     */
    <T> PWindowSource<T> buildSource(SourceFunction<T> sourceFunction, IWindow<T> window);

    /**
     * Returns graph view with view name.
     */
    <K, VV, EV> PGraphView<K, VV, EV> getGraphView(String viewName);

    /**
     * Create graph view with view desc.
     */
    <K, VV, EV> PGraphView<K, VV, EV> createGraphView(IViewDesc viewDesc);

    /**
     * Build window stream graph.
     */
    <K, VV, EV> PGraphWindow<K, VV, EV> buildWindowStreamGraph(PWindowStream<IVertex<K, VV>> vertexWindowSteam,
                                                               PWindowStream<IEdge<K, EV>> edgeWindowStream,
                                                               GraphViewDesc graphViewDesc);
}
