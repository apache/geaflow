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

package org.apache.geaflow.store.lmdb.proxy;

import static org.apache.geaflow.store.lmdb.LmdbConfigKeys.EDGE_CF;
import static org.apache.geaflow.store.lmdb.LmdbConfigKeys.VERTEX_CF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.graph.encoder.IEdgeKVEncoder;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.graph.encoder.IVertexKVEncoder;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.iterator.IteratorWithFlatFn;
import org.apache.geaflow.state.iterator.IteratorWithFn;
import org.apache.geaflow.state.iterator.IteratorWithFnThenFilter;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.StatePushDown;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.iterator.EdgeListScanIterator;
import org.apache.geaflow.store.iterator.EdgeScanIterator;
import org.apache.geaflow.store.iterator.KeysIterator;
import org.apache.geaflow.store.iterator.OneDegreeGraphScanIterator;
import org.apache.geaflow.store.iterator.VertexScanIterator;
import org.apache.geaflow.store.lmdb.LmdbClient;
import org.apache.geaflow.store.lmdb.LmdbIterator;

public class SyncGraphLmdbProxy<K, VV, EV> implements IGraphLmdbProxy<K, VV, EV> {

    protected final Configuration config;
    protected final IVertexKVEncoder<K, VV> vertexEncoder;
    protected final IEdgeKVEncoder<K, EV> edgeEncoder;
    protected IGraphKVEncoder<K, VV, EV> encoder;
    protected final LmdbClient lmdbClient;

    public SyncGraphLmdbProxy(LmdbClient lmdbClient, IGraphKVEncoder<K, VV, EV> encoder,
                                 Configuration config) {
        this.encoder = encoder;
        this.vertexEncoder = this.encoder.getVertexEncoder();
        this.edgeEncoder = this.encoder.getEdgeEncoder();
        this.lmdbClient = lmdbClient;
        this.config = config;
    }

    @Override
    public LmdbClient getClient() {
        return lmdbClient;
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        Tuple<byte[], byte[]> tuple = vertexEncoder.format(vertex);
        this.lmdbClient.write(VERTEX_CF, tuple.f0, tuple.f1);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        Tuple<byte[], byte[]> tuple = edgeEncoder.format(edge);
        this.lmdbClient.write(EDGE_CF, tuple.f0, tuple.f1);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        byte[] value = this.lmdbClient.get(VERTEX_CF, key);
        if (value != null) {
            IVertex<K, VV> vertex = vertexEncoder.getVertex(key, value);
            if (pushdown == null || ((IGraphFilter) pushdown.getFilter()).filterVertex(vertex)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        return getEdges(sid, filter);
    }

    protected List<IEdge<K, EV>> getEdges(K sid, IGraphFilter filter) {
        List<IEdge<K, EV>> list = new ArrayList<>();
        byte[] prefix = edgeEncoder.getScanBytes(sid);
        try (LmdbIterator it = this.lmdbClient.getIterator(EDGE_CF, prefix)) {
            getEdgesFromRocksDBIterator(list, it, filter);
        }

        return list;
    }

    protected void getEdgesFromRocksDBIterator(List<IEdge<K, EV>> list, LmdbIterator it,
                                               IGraphFilter filter) {
        while (it.hasNext()) {
            Tuple<byte[], byte[]> pair = it.next();
            IEdge<K, EV> edge = edgeEncoder.getEdge(pair.f0, pair.f1);
            if (filter.filterEdge(edge)) {
                list.add(edge);
            }
            if (filter.dropAllRemaining()) {
                break;
            }
        }
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(sid, pushdown);
        List<IEdge<K, EV>> edgeList = getEdges(sid, pushdown);
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        OneDegreeGraph<K, VV, EV> oneDegreeGraph = new OneDegreeGraph<>(sid, vertex,
            IteratorWithClose.wrap(edgeList.iterator()));
        if (filter.filterOneDegreeGraph(oneDegreeGraph)) {
            return oneDegreeGraph;
        } else {
            return null;
        }
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        flush();
        LmdbIterator it = this.lmdbClient.getIterator(VERTEX_CF);
        return buildVertexIDIteratorFromRocksDBIter(it);
    }

    protected CloseableIterator<K> buildVertexIDIteratorFromRocksDBIter(
        CloseableIterator<Tuple<byte[], byte[]>> it) {
        return new IteratorWithFnThenFilter<>(it, tuple2 -> vertexEncoder.getVertexID(tuple2.f0),
            predicate());
    }

    private Predicate<K> predicate() {
        return new Predicate<K>() {
            K last = null;

            @Override
            public boolean test(K k) {
                boolean res = k.equals(last);
                last = k;
                return !res;
            }
        };
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        if (pushDown.getFilter() == null) {
            return vertexIDIterator();
        } else {
            return new IteratorWithFn<>(getVertexIterator(pushDown), IVertex::getId);
        }
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        flush();
        LmdbIterator it = lmdbClient.getIterator(VERTEX_CF);
        return new VertexScanIterator<>(it, pushdown, vertexEncoder::getVertex);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getVertex, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        flush();
        LmdbIterator it = lmdbClient.getIterator(EDGE_CF);
        return new EdgeScanIterator<>(it, pushdown, edgeEncoder::getEdge);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return new IteratorWithFlatFn<>(new KeysIterator<>(keys, this::getEdges, pushdown), List::iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        flush();
        return new OneDegreeGraphScanIterator<>(encoder.getKeyType(),
            getVertexIterator(pushdown), getEdgeIterator(pushdown), pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        return new KeysIterator<>(keys, this::getOneDegreeGraph, pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        flush();
        return new IteratorWithFn<>(getEdgeIterator(pushdown), e -> Tuple.of(e.getSrcId(), pushdown.getProjector().project(e)));
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return new IteratorWithFn<>(getEdgeIterator(keys, pushdown), e -> Tuple.of(e.getSrcId(), pushdown.getProjector().project(e)));
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>();
        Iterator<List<IEdge<K, EV>>> it =
            new EdgeListScanIterator<>(getEdgeIterator(pushdown));
        while (it.hasNext()) {
            List<IEdge<K, EV>> edges = it.next();
            K key = edges.get(0).getSrcId();
            res.put(key, (long) edges.size());
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
            pushdownFun =
                key -> StatePushDown.of().withFilter((IFilter) pushdown.getFilters().get(key));
        }

        for (K key : keys) {
            List<IEdge<K, EV>> list = getEdges(key, pushdownFun.apply(key));
            res.put(key, (long) list.size());
        }
        return res;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

}
