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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.iterator.IteratorWithFilterThenFn;
import org.apache.geaflow.state.iterator.IteratorWithFlatFn;
import org.apache.geaflow.state.iterator.IteratorWithFn;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.api.graph.BaseGraphStore;
import org.apache.geaflow.store.api.graph.IDynamicGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.iterator.KeysIterator;
import org.apache.geaflow.store.memory.iterator.MemoryEdgeScanPushDownIterator;
import org.apache.geaflow.store.memory.iterator.MemoryVertexScanIterator;

public class DynamicGraphMemoryStore<K, VV, EV> extends BaseGraphStore implements IDynamicGraphStore<K, VV, EV> {

    private Map<K, Map<Long, List<IEdge<K, EV>>>> vertexId2Edges = new ConcurrentHashMap<>();
    private Map<K, Map<Long, IVertex<K, VV>>> vertexId2Vertex = new ConcurrentHashMap<>();

    public DynamicGraphMemoryStore() {
    }

    @Override
    public void init(StoreContext context) {
        super.init(context);
    }

    @Override
    public void archive(long checkpointId) {

    }

    @Override
    public void recovery(long checkpointId) {

    }

    @Override
    public long recoveryLatest() {
        return 0;
    }

    @Override
    public void compact() {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        this.vertexId2Edges.clear();
        this.vertexId2Vertex.clear();
    }

    @Override
    public void drop() {
        this.vertexId2Edges = null;
        this.vertexId2Vertex = null;
    }

    public Map<K, List<IEdge<K, EV>>> getVertexId2Edges(long version) {
        Map<K, List<IEdge<K, EV>>> map = new HashMap<>(vertexId2Edges.size());
        for (Entry<K, Map<Long, List<IEdge<K, EV>>>> entry : vertexId2Edges.entrySet()) {
            if (entry.getValue().containsKey(version)) {
                map.put(entry.getKey(), entry.getValue().get(version));
            }

        }
        return map;
    }

    public Map<K, IVertex<K, VV>> getVertexId2Vertex(long version) {
        Map<K, IVertex<K, VV>> map = new HashMap<>(vertexId2Vertex.size());
        for (Entry<K, Map<Long, IVertex<K, VV>>> entry : vertexId2Vertex.entrySet()) {
            if (entry.getValue().containsKey(version)) {
                map.put(entry.getKey(), entry.getValue().get(version));
            }
        }
        return map;
    }

    private Iterator<K> getKeyIterator() {
        Set<K> keys = new HashSet<>(vertexId2Vertex.keySet());
        keys.addAll(vertexId2Edges.keySet());
        return keys.iterator();
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        K srcId = edge.getSrcId();
        Map<Long, List<IEdge<K, EV>>> map = vertexId2Edges.computeIfAbsent(srcId,
            k -> new HashMap<>());
        List<IEdge<K, EV>> edges = map.computeIfAbsent(version, k -> new ArrayList<>());
        edges.add(edge);
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        K id = vertex.getId();
        Map<Long, IVertex<K, VV>> map = vertexId2Vertex.computeIfAbsent(id, k -> new HashMap<>());
        map.put(version, vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown) {
        Map<Long, IVertex<K, VV>> map = vertexId2Vertex.get(sid);
        IVertex<K, VV> vertex = null;
        if (map != null) {
            vertex = map.get(version);
        }
        if (vertex != null && ((IGraphFilter) pushdown.getFilter()).filterVertex(vertex)) {
            return vertex;
        }
        return null;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown) {
        Map<Long, List<IEdge<K, EV>>> map = vertexId2Edges.get(sid);
        List<IEdge<K, EV>> list = new ArrayList<>();
        if (map != null) {
            list = map.getOrDefault(version, new ArrayList<>());
        }
        IGraphFilter filter = (IGraphFilter) pushdown.getFilter();
        return list.stream().filter(filter::filterEdge).collect(Collectors.toList());
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long version, K sid,
                                                       IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(version, sid, pushdown);
        List<IEdge<K, EV>> edgeList = getEdges(version, sid, pushdown);
        OneDegreeGraph<K, VV, EV> oneDegreeGraph = new OneDegreeGraph<>(sid, vertex,
            IteratorWithClose.wrap(edgeList.iterator()));
        if (((IGraphFilter) pushdown.getFilter()).filterOneDegreeGraph(oneDegreeGraph)) {
            return oneDegreeGraph;
        } else {
            return null;
        }
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return IteratorWithClose.wrap(vertexId2Vertex.keySet().iterator());
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(long version, IStatePushDown pushdown) {
        if (pushdown.getFilter() == null) {
            return new IteratorWithFilterThenFn<>(vertexId2Vertex.entrySet().iterator(),
                entry -> entry.getValue().containsKey(version), Entry::getKey);
        } else {
            return new IteratorWithFn<>(getVertexIterator(version, pushdown), IVertex::getId);
        }
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version,
                                                               IStatePushDown pushdown) {
        return new MemoryVertexScanIterator<>(getVertexId2Vertex(version).values().iterator(),
            (IGraphFilter) pushdown.getFilter());
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                               IStatePushDown pushdown) {
        BiFunction<K, IStatePushDown, IVertex<K, VV>> fetchFun = (k, p) -> getVertex(version, k, p);
        return new KeysIterator<>(keys, fetchFun, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        Iterator<List<IEdge<K, EV>>> it = new MemoryEdgeScanPushDownIterator<>(
            getVertexId2Edges(version).values().iterator(), pushdown);
        return new IteratorWithFlatFn<>(it, List::iterator);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                           IStatePushDown pushdown) {
        BiFunction<K, IStatePushDown, List<IEdge<K, EV>>> fetchFun = (k, p) -> getEdges(version, k,
            p);
        Iterator<List<IEdge<K, EV>>> it = new KeysIterator<>(keys, fetchFun, pushdown);
        return new IteratorWithFlatFn<>(it, List::iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                                  IStatePushDown pushdown) {
        BiFunction<K, IStatePushDown, OneDegreeGraph<K, VV, EV>> fetchFun =
            (k, p) -> getOneDegreeGraph(
                version, k, p);

        return new KeysIterator<>(Lists.newArrayList(getKeyIterator()), fetchFun, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                                  List<K> keys,
                                                                                  IStatePushDown pushdown) {
        BiFunction<K, IStatePushDown, OneDegreeGraph<K, VV, EV>> fetchFun =
            (k, p) -> getOneDegreeGraph(
                version, k, p);
        return new KeysIterator<>(keys, fetchFun, pushdown);
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            Map<Long, IVertex<K, VV>> map = vertexId2Vertex.get(id);
            if (map != null) {
                return new ArrayList<>(map.keySet());
            } else {
                return new ArrayList<>();
            }
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public long getLatestVersion(K id, DataType dataType) {
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            Map<Long, IVertex<K, VV>> map = vertexId2Vertex.get(id);
            if (map != null) {
                return map.keySet().stream().max(Long::compare).get();
            } else {
                return -1;
            }
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown,
                                                       DataType dataType) {
        if (dataType != DataType.V) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
        Map<Long, IVertex<K, VV>> map = vertexId2Vertex.get(id);
        Map<Long, IVertex<K, VV>> res = new HashMap<>(map.size());
        IGraphFilter graphFilter = (IGraphFilter) pushdown.getFilter();
        for (Entry<Long, IVertex<K, VV>> entry : map.entrySet()) {
            if (graphFilter.filterVertex(entry.getValue())) {
                res.put(entry.getKey(), entry.getValue());
            }
        }
        return res;
    }

    @Override
    public Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> versions,
                                                    IStatePushDown pushdown, DataType dataType) {
        if (dataType != DataType.V) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
        Map<Long, IVertex<K, VV>> map = vertexId2Vertex.get(id);
        Map<Long, IVertex<K, VV>> res = new HashMap<>(versions.size());
        IGraphFilter graphFilter = (IGraphFilter) pushdown.getFilter();
        for (long version : versions) {
            IVertex<K, VV> vertex = map.get(version);
            if (vertex != null && graphFilter.filterVertex(vertex)) {
                res.put(version, vertex);
            }
        }
        return res;
    }
}
