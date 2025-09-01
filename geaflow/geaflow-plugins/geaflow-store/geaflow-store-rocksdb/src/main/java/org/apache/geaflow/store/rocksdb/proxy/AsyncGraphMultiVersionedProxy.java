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

package org.apache.geaflow.store.rocksdb.proxy;

import static org.apache.geaflow.store.rocksdb.RocksdbConfigKeys.EDGE_CF;
import static org.apache.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_CF;
import static org.apache.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_INDEX_CF;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.data.AsyncFlushMultiVersionedBuffer;
import org.apache.geaflow.store.data.GraphWriteMultiVersionedBuffer;
import org.apache.geaflow.store.rocksdb.RocksdbClient;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.WriteBatch;

public class AsyncGraphMultiVersionedProxy<K, VV, EV> extends SyncGraphMultiVersionedProxy<K, VV, EV> {

    private final AsyncFlushMultiVersionedBuffer<K, VV, EV> flushBuffer;

    public AsyncGraphMultiVersionedProxy(RocksdbClient rocksdbStore,
                                         IGraphKVEncoder<K, VV, EV> encoder,
                                         Configuration config) {
        super(rocksdbStore, encoder, config);
        this.flushBuffer = new AsyncFlushMultiVersionedBuffer<>(config, this::flush, SerializerFactory.getKryoSerializer());
    }

    private void flush(GraphWriteMultiVersionedBuffer<K, VV, EV> graphWriteBuffer) {
        if (graphWriteBuffer.getSize() == 0) {
            return;
        }
        ColumnFamilyHandle vertexCF = this.rocksdbClient.getColumnFamilyHandleMap().get(VERTEX_CF);
        ColumnFamilyHandle indexCF = this.rocksdbClient.getColumnFamilyHandleMap().get(VERTEX_INDEX_CF);
        ColumnFamilyHandle edgeCF = this.rocksdbClient.getColumnFamilyHandleMap().get(EDGE_CF);
        WriteBatch writeBatch = new WriteBatch();
        try {
            for (Entry<K, Map<Long, IVertex<K, VV>>> entry :
                graphWriteBuffer.getVertexId2Vertex().entrySet()) {

                for (Entry<Long, IVertex<K, VV>> innerEntry : entry.getValue().entrySet()) {
                    Tuple<byte[], byte[]> tuple = vertexEncoder.format(innerEntry.getValue());
                    byte[] bVersion = getBinaryVersion(innerEntry.getKey());
                    writeBatch.put(vertexCF, concat(bVersion, tuple.f0), tuple.f1);
                    writeBatch.put(indexCF, concat(tuple.f0, bVersion), EMPTY_BYTES);
                }
            }
            for (Entry<K, Map<Long, List<IEdge<K, EV>>>> entry :
                graphWriteBuffer.getVertexId2Edges().entrySet()) {
                for (Entry<Long, List<IEdge<K, EV>>> innerEntry : entry.getValue().entrySet()) {
                    byte[] bVersion = getBinaryVersion(innerEntry.getKey());
                    for (IEdge<K, EV> c : innerEntry.getValue()) {
                        Tuple<byte[], byte[]> tuple = edgeEncoder.format(c);
                        writeBatch.put(edgeCF, concat(bVersion, tuple.f0), tuple.f1);
                    }
                }
            }
        } catch (Exception ex) {
            throw new GeaflowRuntimeException(ex);
        }
        this.rocksdbClient.write(writeBatch);
        writeBatch.clear();
        writeBatch.close();
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        this.flushBuffer.addVertex(version, vertex);
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        this.flushBuffer.addEdge(version, edge);
    }

    @Override
    public IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = this.flushBuffer.readBufferedVertex(version, sid);
        if (vertex != null) {
            return ((IGraphFilter) pushdown.getFilter()).filterVertex(vertex) ? vertex : null;
        }
        return super.getVertex(version, sid, pushdown);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown) {
        List<IEdge<K, EV>> list = this.flushBuffer.readBufferedEdges(version, sid);
        LinkedHashSet<IEdge<K, EV>> set = new LinkedHashSet<>();
        list.stream().filter(((IGraphFilter) pushdown.getFilter())::filterEdge).forEach(set::add);
        set.addAll(super.getEdges(version, sid, pushdown));
        return new ArrayList<>(set);
    }

    @Override
    public void flush() {
        flushBuffer.flush();
    }

    @Override
    public void close() {
        flushBuffer.close();
    }
}
