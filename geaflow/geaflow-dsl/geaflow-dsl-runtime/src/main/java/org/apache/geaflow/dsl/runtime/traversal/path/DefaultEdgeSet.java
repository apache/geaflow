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

package org.apache.geaflow.dsl.runtime.traversal.path;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.dsl.common.data.RowEdge;

public class DefaultEdgeSet implements EdgeSet {

    private final List<RowEdge> edges;

    public DefaultEdgeSet(List<RowEdge> edges) {
        this.edges = Objects.requireNonNull(edges, "edges is null");
    }

    public DefaultEdgeSet() {
        this(new ArrayList<>());
    }

    @Override
    public Iterator<RowEdge> iterator() {
        return edges.iterator();
    }

    @Override
    public void addEdge(RowEdge edge) {
        edges.add(edge);
    }

    @Override
    public int size() {
        return edges.size();
    }

    @Override
    public Object getSrcId() {
        return edges.get(0).getSrcId();
    }

    @Override
    public Object getTargetId() {
        return edges.get(0).getTargetId();
    }

    @Override
    public EdgeSet copy() {
        return new DefaultEdgeSet(Lists.newArrayList(edges));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultEdgeSet)) {
            return false;
        }
        DefaultEdgeSet that = (DefaultEdgeSet) o;
        return Objects.equals(edges, that.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edges);
    }

    @Override
    public String toString() {
        return getSrcId() + "#" + getTargetId() + "(size:" + size() + ")";
    }

    @Override
    public void addEdges(EdgeSet edgeSet) {
        for (RowEdge edge : edgeSet) {
            addEdge(edge);
        }
    }

    @Override
    public boolean like(EdgeSet other) {
        return Objects.equals(getSrcId(), other.getSrcId())
            && Objects.equals(getTargetId(), other.getTargetId());
    }
}
