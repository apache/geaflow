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
import java.util.Objects;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

public class GCNEdgeRecord implements Serializable {

    private final Object srcId;
    private final Object targetId;
    private final String label;
    private final EdgeDirection direction;
    private final double weight;

    public GCNEdgeRecord(Object srcId, Object targetId, String label, EdgeDirection direction,
                         double weight) {
        this.srcId = srcId;
        this.targetId = targetId;
        this.label = label;
        this.direction = direction;
        this.weight = weight;
    }

    public Object getSrcId() {
        return srcId;
    }

    public Object getTargetId() {
        return targetId;
    }

    public String getLabel() {
        return label;
    }

    public EdgeDirection getDirection() {
        return direction;
    }

    public double getWeight() {
        return weight;
    }

    public String identity() {
        return String.valueOf(srcId) + "->" + targetId + "@" + label + "#" + direction;
    }

    public GCNEdgeRecord mergeWeight(double delta) {
        return new GCNEdgeRecord(srcId, targetId, label, direction, weight + delta);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GCNEdgeRecord)) {
            return false;
        }
        GCNEdgeRecord that = (GCNEdgeRecord) o;
        return Objects.equals(srcId, that.srcId) && Objects.equals(targetId, that.targetId)
            && Objects.equals(label, that.label) && direction == that.direction
            && Double.compare(weight, that.weight) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcId, targetId, label, direction, weight);
    }
}
