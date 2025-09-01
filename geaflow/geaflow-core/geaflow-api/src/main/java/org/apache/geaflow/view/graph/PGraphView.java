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

package org.apache.geaflow.view.graph;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.view.PView;

public interface PGraphView<K, VV, EV> extends PView {

    /**
     * Load and initialize graph view.
     */
    static <K, VV, EV> PGraphView<K, VV, EV> loadPGraphView(GraphViewDesc graphViewDesc) {
        ServiceLoader<PGraphView> contextLoader = ServiceLoader.load(PGraphView.class);
        Iterator<PGraphView> graphViewIterable = contextLoader.iterator();
        while (graphViewIterable.hasNext()) {
            PGraphView<K, VV, EV> pGraphView = graphViewIterable.next();
            pGraphView.init(graphViewDesc);
            return pGraphView;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(PGraphView.class.getSimpleName()));
    }

    /**
     * Initialize graph view by desc.
     */
    PGraphView<K, VV, EV> init(GraphViewDesc graphViewDesc);

    /**
     * Append vertex stream into incremental graphView.
     */
    PIncGraphView<K, VV, EV> appendVertex(PWindowStream<IVertex<K, VV>> vertexStream);

    /**
     * Append edge stream into incremental graphView.
     */
    PIncGraphView<K, VV, EV> appendEdge(PWindowStream<IEdge<K, EV>> edgeStream);

    /**
     * Append vertex/edge stream into incremental graphView.
     */
    PIncGraphView<K, VV, EV> appendGraph(PWindowStream<IVertex<K, VV>> vertexStream,
                                         PWindowStream<IEdge<K, EV>> edgeStream);

    /**
     * Build graph snapshot of specified version.
     */
    PGraphWindow<K, VV, EV> snapshot(long version);

}
