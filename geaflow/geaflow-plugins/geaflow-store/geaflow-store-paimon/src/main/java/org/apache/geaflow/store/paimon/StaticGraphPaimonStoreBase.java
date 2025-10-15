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

package org.apache.geaflow.store.paimon;

import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.graph.encoder.EdgeAtom;
import org.apache.geaflow.state.graph.encoder.GraphKVEncoderFactory;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.store.api.graph.IStaticGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.paimon.proxy.IGraphPaimonProxy;
import org.apache.geaflow.store.paimon.proxy.PaimonProxyBuilder;

public class StaticGraphPaimonStoreBase<K, VV, EV> extends BasePaimonGraphStore implements
    IStaticGraphStore<K, VV, EV> {

    private EdgeAtom sortAtom;

    private IGraphPaimonProxy<K, VV, EV> proxy;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        int[] projection = new int[]{KEY_COLUMN_INDEX, VALUE_COLUMN_INDEX};

        PaimonTableRWHandle vertexHandle = createVertexTable(shardId);
        PaimonTableRWHandle edgeHandle = createEdgeTable(shardId);

        this.sortAtom = storeContext.getGraphSchema().getEdgeAtoms().get(1);
        IGraphKVEncoder<K, VV, EV> encoder = GraphKVEncoderFactory.build(storeContext.getConfig(),
            storeContext.getGraphSchema());
        this.proxy = PaimonProxyBuilder.build(storeContext.getConfig(), vertexHandle, edgeHandle,
            projection, encoder);
    }

    @Override
    public void archive(long checkpointId) {
        this.proxy.archive(checkpointId);
    }

    @Override
    public void recovery(long checkpointId) {
        // TODO: Not implemented yet.
        this.proxy.recover(checkpointId);
    }

    @Override
    public long recoveryLatest() {
        return this.proxy.recoverLatest();
    }

    @Override
    public void compact() {

    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        this.proxy.addEdge(edge);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        this.proxy.addVertex(vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        return this.proxy.getVertex(sid, pushdown);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return this.proxy.getEdges(sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return this.proxy.getOneDegreeGraph(sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return this.proxy.vertexIDIterator();
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        return this.proxy.vertexIDIterator(pushDown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        return this.proxy.getVertexIterator(pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys,
                                                               IStatePushDown pushdown) {
        return this.proxy.getVertexIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        return this.proxy.getEdgeIterator(pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return this.proxy.getEdgeIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        return this.proxy.getOneDegreeGraphIterator(pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys,
                                                                                  IStatePushDown pushdown) {
        return this.proxy.getOneDegreeGraphIterator(keys, pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return this.proxy.getEdgeProjectIterator(pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K,
                                                                         EV>, R> pushdown) {
        return this.proxy.getEdgeProjectIterator(keys, pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        return this.proxy.getAggResult(pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        return this.proxy.getAggResult(keys, pushdown);
    }

    @Override
    public void flush() {
        this.proxy.flush();
    }

    @Override
    public void drop() {
        super.drop();
    }

    @Override
    public void close() {
        proxy.close();
        super.close();
    }

    private void checkOrderField(List<EdgeAtom> orderFields) {
        boolean emptyFields = orderFields == null || orderFields.isEmpty();
        boolean checkOk = emptyFields || sortAtom == orderFields.get(0);
        if (!checkOk) {
            throw new GeaflowRuntimeException(
                String.format("store is sort by %s but need %s", sortAtom, orderFields.get(0)));
        }
    }
}
