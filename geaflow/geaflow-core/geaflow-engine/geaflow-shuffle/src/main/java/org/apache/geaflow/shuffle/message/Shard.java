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

package org.apache.geaflow.shuffle.message;

import java.io.Serializable;
import java.util.List;

public class Shard implements Serializable {

    // An edgeId can identity the relationship between upstream and downstream tasks.
    private final int edgeId;

    // All output slices of the edge.
    private final List<ISliceMeta> slices;

    public Shard(int edgeId, List<ISliceMeta> slices) {
        this.edgeId = edgeId;
        this.slices = slices;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public List<ISliceMeta> getSlices() {
        return slices;
    }
}
