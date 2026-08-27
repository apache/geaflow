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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GCNFragmentMessage implements Serializable {

    private final Object rootId;
    private final Object vertexId;
    private final List<Double> features;
    private final List<GCNEdgeRecord> edges;
    private final boolean dynamicExecution;

    public GCNFragmentMessage(Object rootId, Object vertexId, List<Double> features,
                              List<GCNEdgeRecord> edges, boolean dynamicExecution) {
        this.rootId = rootId;
        this.vertexId = vertexId;
        this.features = new ArrayList<>(features);
        this.edges = new ArrayList<>(edges);
        this.dynamicExecution = dynamicExecution;
    }

    public Object getRootId() {
        return rootId;
    }

    public Object getVertexId() {
        return vertexId;
    }

    public List<Double> getFeatures() {
        return Collections.unmodifiableList(features);
    }

    public List<GCNEdgeRecord> getEdges() {
        return Collections.unmodifiableList(edges);
    }

    public boolean isDynamicExecution() {
        return dynamicExecution;
    }
}
