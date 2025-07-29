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

package org.apache.geaflow.api.function.io;

import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.model.graph.GraphRecord;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public interface GraphSourceFunction<K, VV, EV> extends SourceFunction<GraphRecord<IVertex<K, VV>, IEdge<K, EV>>> {

    /**
     * Fetch vertex/edge data from source by window, and collect data by ctx.
     */
    @Override
    default boolean fetch(IWindow<GraphRecord<IVertex<K, VV>, IEdge<K, EV>>> window,
                          SourceContext<GraphRecord<IVertex<K, VV>, IEdge<K, EV>>> ctx) throws Exception {
        return fetch(window.windowId(), (GraphSourceContext<K, VV, EV>) ctx);
    }

    boolean fetch(long windowId, GraphSourceContext<K, VV, EV> ctx) throws Exception;

    interface GraphSourceContext<K, VV, EV> extends SourceContext<GraphRecord<IVertex<K, VV>,
        IEdge<K, EV>>> {

        /**
         * Partition vertex.
         */
        void collectVertex(IVertex<K, VV> vertex) throws Exception;

        /**
         * Partition edge.
         */
        void collectEdge(IEdge<K, EV> edge) throws Exception;
    }
}
