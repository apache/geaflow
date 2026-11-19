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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GCNAccumulator implements Serializable {

    private final Object rootId;
    private final Map<Object, List<Double>> nodeFeatures;
    private final Map<String, GCNEdgeRecord> edgeRecords;

    public GCNAccumulator(Object rootId) {
        this.rootId = rootId;
        this.nodeFeatures = new LinkedHashMap<>();
        this.edgeRecords = new LinkedHashMap<>();
    }

    public Object getRootId() {
        return rootId;
    }

    public Map<Object, List<Double>> getNodeFeatures() {
        return nodeFeatures;
    }

    public List<GCNEdgeRecord> getEdgeRecords() {
        return new ArrayList<>(edgeRecords.values());
    }

    public void addFragment(GCNFragmentMessage fragmentMessage) {
        nodeFeatures.put(fragmentMessage.getVertexId(),
            new ArrayList<>(fragmentMessage.getFeatures()));
        for (GCNEdgeRecord edgeRecord : fragmentMessage.getEdges()) {
            String identity = edgeRecord.identity();
            GCNEdgeRecord existing = edgeRecords.get(identity);
            if (existing == null) {
                edgeRecords.put(identity, edgeRecord);
            } else {
                edgeRecords.put(identity, existing.mergeWeight(edgeRecord.getWeight()));
            }
        }
    }
}
