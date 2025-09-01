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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.graph.encoder.GraphKVEncoderFactory;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.store.api.graph.IDynamicGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.paimon.proxy.IGraphMultiVersionedPaimonProxy;
import org.apache.geaflow.store.paimon.proxy.PaimonProxyBuilder;
import org.apache.paimon.catalog.Identifier;

public class DynamicGraphPaimonStoreBase<K, VV, EV> extends BasePaimonGraphStore implements
    IDynamicGraphStore<K, VV, EV> {

    private IGraphMultiVersionedPaimonProxy<K, VV, EV> proxy;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        int[] projection = new int[]{KEY_COLUMN_INDEX, VALUE_COLUMN_INDEX};

        // TODO: Use graph schema to create table instead of KV table.
        String vertexTableName = String.format("%s#%s", "vertex", shardId);
        Identifier vertexIdentifier = new Identifier(paimonStoreName, vertexTableName);
        PaimonTableRWHandle vertexHandle = createKVTableHandle(vertexIdentifier);

        String vertexIndexTableName = String.format("%s#%s", "vertex_index", shardId);
        Identifier vertexIndexIdentifier = new Identifier(paimonStoreName, vertexIndexTableName);
        PaimonTableRWHandle vertexIndexHandle = createKVTableHandle(vertexIndexIdentifier);

        String edgeTableName = String.format("%s#%s", "edge", shardId);
        Identifier edgeIdentifier = new Identifier(paimonStoreName, edgeTableName);
        PaimonTableRWHandle edgeHandle = createKVTableHandle(edgeIdentifier);

        IGraphKVEncoder<K, VV, EV> encoder = GraphKVEncoderFactory.build(storeContext.getConfig(),
            storeContext.getGraphSchema());
        this.proxy = PaimonProxyBuilder.buildMultiVersioned(storeContext.getConfig(), vertexHandle,
            vertexIndexHandle, edgeHandle, projection, encoder);
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
    public void addEdge(long version, IEdge<K, EV> edge) {
        this.proxy.addEdge(version, edge);
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        this.proxy.addVertex(version, vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(long sliceId, K sid, IStatePushDown pushdown) {
        return this.proxy.getVertex(sliceId, sid, pushdown);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long sliceId, K sid, IStatePushDown pushdown) {
        return this.proxy.getEdges(sliceId, sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long sliceId, K sid,
                                                       IStatePushDown pushdown) {
        return this.proxy.getOneDegreeGraph(sliceId, sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return this.proxy.vertexIDIterator();
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(long version, IStatePushDown pushdown) {
        return this.proxy.vertexIDIterator(version, pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version,
                                                               IStatePushDown pushdown) {
        return proxy.getVertexIterator(version, pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                               IStatePushDown pushdown) {
        return proxy.getVertexIterator(version, keys, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        return proxy.getEdgeIterator(version, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                           IStatePushDown pushdown) {
        return proxy.getEdgeIterator(version, keys, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                                  IStatePushDown pushdown) {
        return proxy.getOneDegreeGraphIterator(version, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                                  List<K> keys,
                                                                                  IStatePushDown pushdown) {
        return proxy.getOneDegreeGraphIterator(version, keys, pushdown);
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        return this.proxy.getAllVersions(id, dataType);
    }

    @Override
    public long getLatestVersion(K id, DataType dataType) {
        return this.proxy.getLatestVersion(id, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown,
                                                       DataType dataType) {
        return this.proxy.getAllVersionData(id, pushdown, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> slices,
                                                    IStatePushDown pushdown, DataType dataType) {
        return this.proxy.getVersionData(id, slices, pushdown, dataType);
    }

    @Override
    public void flush() {
        proxy.flush();
    }

    @Override
    public void close() {
        proxy.close();
        super.close();
    }
}
