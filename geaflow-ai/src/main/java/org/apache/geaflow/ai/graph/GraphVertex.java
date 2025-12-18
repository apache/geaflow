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

package org.apache.geaflow.ai.graph;

import java.util.Objects;
import org.apache.geaflow.ai.graph.io.Vertex;

public class GraphVertex implements GraphEntity {

    public final Vertex vertex;

    public GraphVertex(Vertex vertex) {
        this.vertex = vertex;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String getLabel() {
        return vertex.getLabel();
    }

    @Override
    public String toString() {
        return vertex.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphVertex that = (GraphVertex) o;
        return Objects.equals(vertex, that.vertex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertex);
    }
}
