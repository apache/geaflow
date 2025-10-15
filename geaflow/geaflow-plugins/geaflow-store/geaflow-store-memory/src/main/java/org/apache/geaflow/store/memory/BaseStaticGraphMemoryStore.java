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

package org.apache.geaflow.store.memory;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.graph.encoder.EdgeAtom;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.iterator.IteratorWithFlatFn;
import org.apache.geaflow.state.iterator.IteratorWithFn;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.StatePushDown;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.api.graph.BaseGraphStore;
import org.apache.geaflow.store.api.graph.IStaticGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.iterator.KeysIterator;
import org.apache.geaflow.store.memory.iterator.MemoryEdgeScanPushDownIterator;
import org.apache.geaflow.store.memory.iterator.MemoryVertexScanIterator;

public abstract class BaseStaticGraphMemoryStore<K, VV, EV> extends BaseGraphStore implements
    IStaticGraphStore<K, VV, EV> {

    @Override
    public void init(StoreContext context) {
        super.init(context);
    }

    protected abstract IVertex<K, VV> getVertex(K sid);

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(sid);
        return vertex != null && ((IGraphFilter) pushdown.getFilter()).filterVertex(vertex) ? vertex : null;
    }

    protected abstract List<IEdge<K, EV>> getEdges(K sid);

    protected List<IEdge<K, EV>> pushdownEdges(List<IEdge<K, EV>> list, IStatePushDown pushdown) {
        if (pushdown.getOrderFields() != null) {
            list.sort(EdgeAtom.getComparator(pushdown.getOrderFields()));
        }
        List<IEdge<K, EV>> res = new ArrayList<>(list.size());
        Iterator<IEdge<K, EV>> it = list.iterator();
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        while (it.hasNext() && !filter.dropAllRemaining()) {
            IEdge<K, EV> edge = it.next();
            if (filter.filterEdge(edge)) {
                res.add(edge);
            }
        }

        return res;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        return pushdownEdges(getEdges(sid), pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(sid, pushdown);
        List<IEdge<K, EV>> edges = getEdges(sid, pushdown);
        OneDegreeGraph<K, VV, EV> oneDegreeGraph = new OneDegreeGraph<>(sid, vertex,
            IteratorWithClose.wrap(edges.iterator()));
        if (((IGraphFilter) pushdown.getFilter()).filterOneDegreeGraph(oneDegreeGraph)) {
            return oneDegreeGraph;
        }
        return null;
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        boolean emptyFilter = pushdown.getFilter().getFilterType() == FilterType.EMPTY;
        return emptyFilter ? getVertexIterator()
            : new MemoryVertexScanIterator<>(getVertexIterator(), (IGraphFilter) pushdown.getFilter());
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> list, IStatePushDown pushdown) {
        return new KeysIterator<>(list, this::getVertex, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        CloseableIterator<List<IEdge<K, EV>>> it = new MemoryEdgeScanPushDownIterator<>(getEdgesIterator(), pushdown);
        return new IteratorWithFlatFn<>(it, List::iterator);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> list, IStatePushDown pushdown) {
        CloseableIterator<List<IEdge<K, EV>>> it = new KeysIterator<>(list, this::getEdges, pushdown);
        return new IteratorWithFlatFn<>(it, List::iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(IStatePushDown pushdown) {
        return new KeysIterator<>(Lists.newArrayList(getKeyIterator()), this::getOneDegreeGraph, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getOneDegreeGraph, pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return new IteratorWithFn<>(getEdgeIterator(pushdown),
            edge -> Tuple.of(edge.getSrcId(), pushdown.getProjector().project(edge)));
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return new IteratorWithFn<>(getEdgeIterator(keys, pushdown),
            edge -> Tuple.of(edge.getSrcId(), pushdown.getProjector().project(edge)));
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>();
        Iterator<K> keyIt = getKeyIterator();
        while (keyIt.hasNext()) {
            K key = keyIt.next();

            List<IEdge<K, EV>> list = getEdges(key, pushdown);
            res.put(key, (long) list.size());
        }
        return res;
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>(keys.size());

        Function<K, IStatePushDown> pushdownFun;
        if (pushdown.getFilters() == null) {
            pushdownFun = key -> pushdown;
        } else {
            pushdownFun = key -> StatePushDown.of().withFilter((IGraphFilter) pushdown.getFilters().get(key));
        }

        for (K key : keys) {
            List<IEdge<K, EV>> list = getEdges(key, pushdownFun.apply(key));
            res.put(key, (long) list.size());
        }
        return res;
    }

    protected abstract CloseableIterator<List<IEdge<K, EV>>> getEdgesIterator();

    protected abstract CloseableIterator<IVertex<K, VV>> getVertexIterator();

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return new IteratorWithFn<>(getVertexIterator(), IVertex::getId);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        if (pushDown.getFilter() == null) {
            return vertexIDIterator();
        } else {
            return new IteratorWithFn<>(getVertexIterator(pushDown), IVertex::getId);
        }
    }

    protected abstract CloseableIterator<K> getKeyIterator();
}
