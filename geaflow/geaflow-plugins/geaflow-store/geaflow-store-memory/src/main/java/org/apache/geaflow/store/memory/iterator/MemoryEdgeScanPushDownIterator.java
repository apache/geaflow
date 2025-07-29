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

package org.apache.geaflow.store.memory.iterator;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.graph.encoder.EdgeAtom;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.state.pushdown.limit.IEdgeLimit;

public class MemoryEdgeScanPushDownIterator<K, VV, EV> implements CloseableIterator<List<IEdge<K, EV>>> {

    private final Iterator<List<IEdge<K, EV>>> iterator;
    private final Comparator<IEdge> edgeComparator;
    private final IEdgeLimit edgeLimit;
    private final IGraphFilter filter;
    private List<IEdge<K, EV>> nextValue;

    public MemoryEdgeScanPushDownIterator(
        Iterator<List<IEdge<K, EV>>> iterator,
        IStatePushDown pushdown) {
        this.filter = (IGraphFilter) pushdown.getFilter();
        this.edgeLimit = pushdown.getEdgeLimit();
        List<EdgeAtom> orderFields = pushdown.getOrderFields();
        this.edgeComparator = EdgeAtom.getComparator(orderFields);
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            List<IEdge<K, EV>> list = Lists.newArrayList(iterator.next());
            if (this.edgeComparator != null) {
                list.sort(this.edgeComparator);
            }
            List<IEdge<K, EV>> res = new ArrayList<>(list.size());
            Iterator<IEdge<K, EV>> it = list.iterator();
            IGraphFilter filter = GraphFilter.of(this.filter, this.edgeLimit);
            while (it.hasNext() && !filter.dropAllRemaining()) {
                IEdge<K, EV> edge = it.next();
                if (filter.filterEdge(edge)) {
                    res.add(edge);
                }
            }

            nextValue = res;
            return true;
        }
        return false;
    }

    @Override
    public List<IEdge<K, EV>> next() {
        return nextValue;
    }

    @Override
    public void close() {

    }
}
