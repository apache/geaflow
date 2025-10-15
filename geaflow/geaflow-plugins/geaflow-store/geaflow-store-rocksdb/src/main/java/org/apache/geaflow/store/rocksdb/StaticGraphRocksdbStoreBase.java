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

package org.apache.geaflow.store.rocksdb;

import static org.apache.geaflow.store.rocksdb.RocksdbConfigKeys.DEFAULT_CF;
import static org.apache.geaflow.store.rocksdb.RocksdbConfigKeys.EDGE_CF;
import static org.apache.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_CF;

import java.util.Arrays;
import java.util.Collections;
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
import org.apache.geaflow.store.rocksdb.proxy.IGraphRocksdbProxy;
import org.apache.geaflow.store.rocksdb.proxy.ProxyBuilder;

public class StaticGraphRocksdbStoreBase<K, VV, EV> extends BaseRocksdbGraphStore implements
    IStaticGraphStore<K, VV, EV> {

    private IGraphRocksdbProxy<K, VV, EV> proxy;
    private EdgeAtom sortAtom;
    private PartitionType partitionType;

    @Override
    public void init(StoreContext storeContext) {
        // Init partition type for rocksdb graph store
        partitionType = PartitionType.getEnum(storeContext.getConfig()
            .getString(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE));

        super.init(storeContext);
        IGraphKVEncoder<K, VV, EV> encoder = GraphKVEncoderFactory.build(config,
            storeContext.getGraphSchema());
        sortAtom = storeContext.getGraphSchema().getEdgeAtoms().get(1);
        this.proxy = ProxyBuilder.build(config, rocksdbClient, encoder);
    }

    @Override
    protected List<String> getCfList() {
        if (!partitionType.isPartition()) {
            return Arrays.asList(VERTEX_CF, EDGE_CF);
        }

        return Collections.singletonList(DEFAULT_CF);
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
        return proxy.getEdges(sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return proxy.getOneDegreeGraph(sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return this.proxy.vertexIDIterator();
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        return proxy.vertexIDIterator(pushDown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        return proxy.getVertexIterator(pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return proxy.getVertexIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return proxy.getEdgeIterator(pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return proxy.getEdgeIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return proxy.getOneDegreeGraphIterator(pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return proxy.getOneDegreeGraphIterator(keys, pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return proxy.getEdgeProjectIterator(pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys, IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return proxy.getEdgeProjectIterator(keys, pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        return proxy.getAggResult(pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        return proxy.getAggResult(keys, pushdown);
    }

    private void checkOrderField(List<EdgeAtom> orderFields) {
        boolean emptyFields = orderFields == null || orderFields.isEmpty();
        boolean checkOk = emptyFields || sortAtom == orderFields.get(0);
        if (!checkOk) {
            throw new GeaflowRuntimeException(String.format("store is sort by %s but need %s", sortAtom, orderFields.get(0)));
        }
    }

    @Override
    public void flush() {
        proxy.flush();
        super.flush();
    }

    @Override
    public void close() {
        proxy.close();
        super.close();
    }
}
