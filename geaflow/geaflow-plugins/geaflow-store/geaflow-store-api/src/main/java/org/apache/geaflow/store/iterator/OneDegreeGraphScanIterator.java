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

package org.apache.geaflow.store.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.iterator.IOneDegreeGraphIterator;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;

// TODO: Implement one degree graph scan iterator for graph proxy partitioned by label
public class OneDegreeGraphScanIterator<K, VV, EV> implements
    IOneDegreeGraphIterator<K, VV, EV> {

    private final CloseableIterator<IVertex<K, VV>> vertexIterator;
    private final CloseableIterator<List<IEdge<K, EV>>> edgeListIterator;
    private final IType<K> keyType;
    private IGraphFilter filter;
    private OneDegreeGraph<K, VV, EV> nextValue;

    private List<IEdge<K, EV>> candidateEdges;
    private IVertex<K, VV> candidateVertex;

    public OneDegreeGraphScanIterator(
        IType<K> keyType,
        CloseableIterator<IVertex<K, VV>> vertexIterator,
        CloseableIterator<IEdge<K, EV>> edgeIterator,
        IStatePushDown pushdown) {
        this.keyType = keyType;
        this.vertexIterator = vertexIterator;
        this.edgeListIterator = new EdgeListScanIterator<>(edgeIterator);
        this.filter = (IGraphFilter) pushdown.getFilter();
    }


    private IVertex<K, VV> getVertexFromIterator() {
        if (vertexIterator.hasNext()) {
            return vertexIterator.next();
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        do {
            candidateVertex = candidateVertex == null ? getVertexFromIterator() : candidateVertex;
            if (candidateEdges == null && edgeListIterator.hasNext()) {
                candidateEdges = edgeListIterator.next();
            } else if (candidateEdges == null) {
                candidateEdges = new ArrayList<>();
            }

            if (candidateEdges.size() > 0 && candidateVertex != null) {
                K edgeKey = candidateEdges.get(0).getSrcId();
                K vertexKey = candidateVertex.getId();
                int res = keyType.compare(edgeKey, vertexKey);
                if (res < 0) {
                    nextValue = new OneDegreeGraph<>(edgeKey, null,
                        IteratorWithClose.wrap(candidateEdges.iterator()));
                    candidateEdges = null;
                } else if (res == 0) {
                    nextValue = new OneDegreeGraph<>(vertexKey, candidateVertex,
                        IteratorWithClose.wrap(candidateEdges.iterator()));
                    candidateVertex = null;
                    candidateEdges = null;
                } else {
                    nextValue = new OneDegreeGraph<>(vertexKey, candidateVertex,
                        IteratorWithClose.wrap(Collections.emptyIterator()));
                    candidateVertex = null;
                }
            } else if (candidateEdges.size() > 0) {
                nextValue = new OneDegreeGraph<>(candidateEdges.get(0).getSrcId(), null,
                    IteratorWithClose.wrap(candidateEdges.iterator()));
                candidateEdges = null;
            } else if (candidateVertex != null) {
                nextValue = new OneDegreeGraph<>(candidateVertex.getId(), candidateVertex,
                    IteratorWithClose.wrap(Collections.emptyIterator()));
                candidateVertex = null;
            } else {
                return false;
            }

            if (!filter.filterOneDegreeGraph(nextValue)) {
                continue;
            }
            return true;
        } while (true);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> next() {
        return nextValue;
    }

    @Override
    public void close() {
        this.vertexIterator.close();
        this.edgeListIterator.close();
    }
}
