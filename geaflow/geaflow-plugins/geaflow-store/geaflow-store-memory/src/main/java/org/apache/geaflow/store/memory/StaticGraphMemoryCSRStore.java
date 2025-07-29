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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.geaflow.collection.array.PrimitiveArray;
import org.apache.geaflow.collection.array.PrimitiveArrayFactory;
import org.apache.geaflow.collection.map.MapFactory;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.iterator.IteratorWithFn;
import org.apache.geaflow.state.iterator.IteratorWithFnThenFilter;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.state.schema.GraphDataSchema;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.memory.csr.edge.EdgeArrayFactory;
import org.apache.geaflow.store.memory.csr.edge.IEdgeArray;
import org.apache.geaflow.store.memory.csr.vertex.IVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.VertexArrayFactory;

public class StaticGraphMemoryCSRStore<K, VV, EV> extends BaseStaticGraphMemoryStore<K, VV, EV> {

    // inner csr store.
    private CSRStore<K, VV, EV> csrStore;
    private boolean isBuilt;
    private List<IVertex<K, VV>> vertexList;
    private List<IEdge<K, EV>> edgesList;

    @Override
    public void init(StoreContext context) {
        super.init(context);
        isBuilt = false;
        vertexList = new ArrayList<>();
        edgesList = new ArrayList<>();
        csrStore = new CSRStore<>(context);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        Preconditions.checkArgument(!isBuilt, "cannot add vertex/edge after flush.");
        edgesList.add(edge);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        Preconditions.checkArgument(!isBuilt, "cannot add vertex/edge after flush.");
        vertexList.add(vertex);
    }

    @Override
    protected IVertex<K, VV> getVertex(K sid) {
        Preconditions.checkArgument(isBuilt, "flush first.");
        return csrStore.getVertex(sid);
    }

    @Override
    protected List<IEdge<K, EV>> getEdges(K sid) {
        Preconditions.checkArgument(isBuilt, "flush first.");
        return csrStore.getEdges(sid);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        int pos = csrStore.getDictId(sid);
        return getOneDegreeGraph(sid, pos, pushdown);
    }

    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, int pos, IStatePushDown pushdown) {
        OneDegreeGraph<K, VV, EV> oneDegreeGraph;
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        if (pos == CSRStore.NON_EXIST) {
            return new OneDegreeGraph<>(sid, null,
                IteratorWithClose.wrap(Collections.emptyIterator()));
        } else {
            IVertex<K, VV> vertex = csrStore.getVertex(sid, pos);
            if (vertex == null || !filter.filterVertex(vertex)) {
                vertex = null;
            }
            List<IEdge<K, EV>> stream = pushdownEdges(csrStore.getEdges(sid, pos), pushdown);
            oneDegreeGraph = new OneDegreeGraph<>(sid, vertex,
                IteratorWithClose.wrap(stream.iterator()));
        }
        return filter.filterOneDegreeGraph(oneDegreeGraph) ? oneDegreeGraph : null;
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
        this.csrStore.build(vertexList, edgesList);
        this.vertexList = null;
        this.edgesList = null;
        this.isBuilt = true;
    }

    @Override
    public void close() {

    }

    @Override
    protected CloseableIterator<List<IEdge<K, EV>>> getEdgesIterator() {
        Preconditions.checkArgument(isBuilt, "flush first.");
        return new IteratorWithFn<>(IntStream.range(0, csrStore.getDict().size()).iterator(),
            p -> csrStore.getEdges(csrStore.reverse.get(p), p));
    }

    @Override
    protected CloseableIterator<IVertex<K, VV>> getVertexIterator() {
        Preconditions.checkArgument(isBuilt, "flush first.");

        return new IteratorWithFnThenFilter<>(
            IntStream.range(0, csrStore.getDict().size()).iterator(),
            p -> csrStore.getVertex(csrStore.reverse.get(p), p), Objects::nonNull);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        Preconditions.checkArgument(isBuilt, "flush first.");

        return new IteratorWithFnThenFilter<>(
            IntStream.range(0, csrStore.getDict().size()).iterator(), p -> {
            K k = csrStore.reverse.get(p);
            return getOneDegreeGraph(k, p, pushdown);
        }, Objects::nonNull);
    }

    @Override
    protected CloseableIterator<K> getKeyIterator() {
        return IteratorWithClose.wrap(csrStore.getDict().keySet().iterator());
    }

    @Override
    public void drop() {
        csrStore.drop();
    }

    public static class CSRStore<K, VV, EV> {

        public static final int NON_EXIST = -1;
        private final Class<K> keyClazz;

        private Map<K, Integer> kDict;
        private IVertexArray<K, VV> vertexArray;
        private IEdgeArray<K, EV> edgeArray;
        private PrimitiveArray<K> reverse;

        public CSRStore(StoreContext context) {
            GraphDataSchema graphDataSchema = context.getGraphSchema();
            this.keyClazz = graphDataSchema.getKeyType().getTypeClass();
            kDict = MapFactory.buildMap(keyClazz, Integer.TYPE);
            vertexArray = VertexArrayFactory.getVertexArray(graphDataSchema);
            edgeArray = EdgeArrayFactory.getEdgeArray(graphDataSchema);
        }

        public int getDictIdOrRegister(K id) {
            int res = kDict.computeIfAbsent(id, k -> kDict.size());
            return res;
        }

        public int getDictId(K id) {
            return kDict.getOrDefault(id, NON_EXIST);
        }

        public Map<K, Integer> getDict() {
            return kDict;
        }

        public List<IEdge<K, EV>> getEdges(K sid) {
            int pos = getDictId(sid);
            return pos == NON_EXIST ? Collections.EMPTY_LIST : getEdges(sid, pos);
        }

        public List<IEdge<K, EV>> getEdges(K sid, int pos) {
            Tuple<Integer, Integer> edgePosRange = vertexArray.getEdgePosRange(pos);
            return edgeArray.getRangeEdges(sid, edgePosRange.f0, edgePosRange.f1);
        }

        public IVertex<K, VV> getVertex(K id) {
            int pos = getDictId(id);
            return pos == NON_EXIST ? null : getVertex(id, pos);
        }

        private IVertex<K, VV> getVertex(K id, int pos) {
            return vertexArray.getVertex(id, pos);
        }

        public void build(List<IVertex<K, VV>> vertexList, List<IEdge<K, EV>> edgesList) {
            List<List<IEdge<K, EV>>> edgesListTmp = new ArrayList<>();
            for (IVertex<K, VV> vertex : vertexList) {
                int dictId = getDictIdOrRegister(vertex.getId());
                edgesListTmp.add(dictId, new LinkedList<>());
            }
            edgesList.forEach(edge -> {
                int oldSize = kDict.size();
                int dictId = getDictIdOrRegister(edge.getSrcId());
                if (dictId > oldSize - 1) { // new register.
                    vertexList.add(dictId, null);
                    edgesListTmp.add(dictId, new LinkedList<>());
                }
                edgesListTmp.get(dictId).add(edge);
            });
            reverse = PrimitiveArrayFactory.getCustomArray(this.keyClazz, kDict.size());
            for (Entry<K, Integer> entry : kDict.entrySet()) {
                reverse.set(entry.getValue(), entry.getKey());
            }

            vertexArray.init(vertexList.size());
            int edgesNum = edgesListTmp.stream().mapToInt(value -> value != null ? value.size() : 0)
                .sum();
            edgeArray.init(keyClazz, edgesNum);
            for (int i = 0; i < vertexList.size(); i++) {
                vertexArray.set(i, vertexList.get(i));
                int nextPos;
                if (edgesListTmp.get(i) != null && !edgesListTmp.get(i).isEmpty()) {
                    for (IEdge<K, EV> edge : edgesListTmp.get(i)) {
                        nextPos = vertexArray.getNextPos(i);
                        edgeArray.set(nextPos, edge);
                        vertexArray.updateVId2EPos(i);
                    }
                }
            }
        }

        public void drop() {
            kDict = null;
            vertexArray = null;
            edgeArray = null;
        }
    }


}
