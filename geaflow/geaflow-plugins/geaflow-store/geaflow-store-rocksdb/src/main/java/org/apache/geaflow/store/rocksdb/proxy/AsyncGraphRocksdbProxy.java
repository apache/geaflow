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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.data.AsyncFlushBuffer;
import org.apache.geaflow.store.data.GraphWriteBuffer;
import org.apache.geaflow.store.rocksdb.RocksdbClient;

public class AsyncGraphRocksdbProxy<K, VV, EV> extends SyncGraphRocksdbProxy<K, VV, EV> {

    private final AsyncFlushBuffer<K, VV, EV> flushBuffer;

    public AsyncGraphRocksdbProxy(RocksdbClient rocksdbClient,
                                  IGraphKVEncoder<K, VV, EV> encoder,
                                  Configuration config) {
        super(rocksdbClient, encoder, config);
        this.flushBuffer = new AsyncFlushBuffer<>(config, this::flush, SerializerFactory.getKryoSerializer());
    }

    private void flush(GraphWriteBuffer<K, VV, EV> graphWriteBuffer) {
        if (graphWriteBuffer.getSize() == 0) {
            return;
        }

        List<Tuple<byte[], byte[]>> list = graphWriteBuffer.getVertexId2Vertex().values()
            .stream().map(v -> vertexEncoder.format(v)).collect(Collectors.toList());
        rocksdbClient.write(VERTEX_CF, list);

        list.clear();
        for (List<IEdge<K, EV>> edges : graphWriteBuffer.getVertexId2Edges().values()) {
            edges.forEach(e -> list.add(edgeEncoder.format(e)));
        }
        rocksdbClient.write(EDGE_CF, list);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        this.flushBuffer.addVertex(vertex);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        this.flushBuffer.addEdge(edge);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = this.flushBuffer.readBufferedVertex(sid);
        if (vertex != null) {
            return ((IGraphFilter) pushdown.getFilter()).filterVertex(vertex) ? vertex : null;
        }
        return super.getVertex(sid, pushdown);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        List<IEdge<K, EV>> list = this.flushBuffer.readBufferedEdges(sid);
        LinkedHashSet<IEdge<K, EV>> set = new LinkedHashSet<>();

        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        Lists.reverse(list).stream().filter(filter::filterEdge).forEach(set::add);
        if (!filter.dropAllRemaining()) {
            set.addAll(super.getEdges(sid, filter));
        }

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
