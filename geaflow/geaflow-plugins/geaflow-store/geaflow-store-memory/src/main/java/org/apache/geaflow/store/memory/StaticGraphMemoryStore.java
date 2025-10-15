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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.iterator.IteratorWithFnThenFilter;
import org.apache.geaflow.store.context.StoreContext;

public class StaticGraphMemoryStore<K, VV, EV> extends BaseStaticGraphMemoryStore<K, VV, EV> {

    protected Map<K, Tuple<IVertex<K, VV>, List<IEdge<K, EV>>>> map;

    public StaticGraphMemoryStore() {
    }

    @Override
    public void init(StoreContext context) {
        super.init(context);
        map = new ConcurrentHashMap<>();
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        K srcId = edge.getSrcId();
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.computeIfAbsent(srcId,
            k -> Tuple.of(null, new ArrayList<>()));
        v.f1.add(edge);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        K srcId = vertex.getId();
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.computeIfAbsent(srcId,
            k -> Tuple.of(null, new ArrayList<>()));
        v.f0 = vertex;
    }

    @Override
    protected IVertex<K, VV> getVertex(K sid) {
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.get(sid);
        return v == null ? null : v.f0;
    }

    @Override
    protected List<IEdge<K, EV>> getEdges(K sid) {
        Tuple<IVertex<K, VV>, List<IEdge<K, EV>>> v = map.get(sid);
        return v == null ? Collections.EMPTY_LIST : v.f1;
    }

    @Override
    protected CloseableIterator<List<IEdge<K, EV>>> getEdgesIterator() {
        return IteratorWithClose.wrap(map.values().stream().map(c -> c.f1).iterator());
    }

    @Override
    protected CloseableIterator<IVertex<K, VV>> getVertexIterator() {
        return new IteratorWithFnThenFilter<>(map.values().iterator(), c -> c.f0, Objects::nonNull);
    }

    @Override
    protected CloseableIterator<K> getKeyIterator() {
        return IteratorWithClose.wrap(map.keySet().iterator());
    }

    @Override
    public void close() {

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
    public void drop() {

    }

}
