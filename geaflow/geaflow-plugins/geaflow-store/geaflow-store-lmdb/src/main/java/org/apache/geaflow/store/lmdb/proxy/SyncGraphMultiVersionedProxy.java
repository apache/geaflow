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
import static org.apache.geaflow.store.lmdb.LmdbConfigKeys.VERTEX_INDEX_CF;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.graph.encoder.IEdgeKVEncoder;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.graph.encoder.IVertexKVEncoder;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.iterator.IteratorWithFlatFn;
import org.apache.geaflow.state.iterator.IteratorWithFn;
import org.apache.geaflow.state.iterator.IteratorWithFnThenFilter;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.iterator.EdgeScanIterator;
import org.apache.geaflow.store.iterator.KeysIterator;
import org.apache.geaflow.store.iterator.OneDegreeGraphScanIterator;
import org.apache.geaflow.store.iterator.VertexScanIterator;
import org.apache.geaflow.store.lmdb.LmdbClient;
import org.apache.geaflow.store.lmdb.LmdbIterator;

public class SyncGraphMultiVersionedProxy<K, VV, EV> implements IGraphMultiVersionedLmdbProxy<K, VV, EV> {

    private static final int VERSION_BYTES_SIZE = Long.BYTES;
    private static final int VERTEX_INDEX_SUFFIX_SIZE =
        VERSION_BYTES_SIZE + StateConfigKeys.DELIMITER.length;
    protected static final byte[] EMPTY_BYTES = new byte[0];
    protected final Configuration config;
    protected LmdbClient lmdbClient;
    protected IGraphKVEncoder<K, VV, EV> encoder;
    protected IEdgeKVEncoder<K, EV> edgeEncoder;
    protected IVertexKVEncoder<K, VV> vertexEncoder;

    public SyncGraphMultiVersionedProxy(LmdbClient rocksdbStore,
                                        IGraphKVEncoder<K, VV, EV> encoder,
                                        Configuration config) {
        this.encoder = encoder;
        this.lmdbClient = rocksdbStore;
        this.vertexEncoder = encoder.getVertexEncoder();
        this.edgeEncoder = encoder.getEdgeEncoder();
        this.config = config;
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        Tuple<byte[], byte[]> tuple = vertexEncoder.format(vertex);
        byte[] bVersion = getBinaryVersion(version);
        this.lmdbClient.write(VERTEX_CF, concat(bVersion, tuple.f0), tuple.f1);
        this.lmdbClient.write(VERTEX_INDEX_CF, concat(tuple.f0, bVersion), EMPTY_BYTES);
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        byte[] bVersion = getBinaryVersion(version);
        Tuple<byte[], byte[]> tuple = edgeEncoder.format(edge);
        this.lmdbClient.write(EDGE_CF, concat(bVersion, tuple.f0), tuple.f1);
    }

    @Override
    public IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        byte[] bVersion = getBinaryVersion(version);
        byte[] value = this.lmdbClient.get(VERTEX_CF, concat(bVersion, key));
        if (value != null) {
            IVertex<K, VV> vertex = vertexEncoder.getVertex(key, value);
            if (pushdown == null || ((IGraphFilter) pushdown.getFilter()).filterVertex(vertex)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown) {
        List<IEdge<K, EV>> list = new ArrayList<>();
        byte[] bVersion = getBinaryVersion(version);
        byte[] prefix = concat(bVersion, edgeEncoder.getScanBytes(sid));

        IGraphFilter filter = (IGraphFilter) pushdown.getFilter();
        try (LmdbIterator it = this.lmdbClient.getIterator(EDGE_CF, prefix)) {
            while (it.hasNext()) {
                Tuple<byte[], byte[]> pair = it.next();
                IEdge<K, EV> edge = edgeEncoder.getEdge(getKeyFromVersionToKey(pair.f0), pair.f1);
                if (filter.filterEdge(edge)) {
                    list.add(edge);
                }
            }
        }
        return list;
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long version, K sid, IStatePushDown pushdown) {
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
        flush();
        LmdbIterator it = this.lmdbClient.getIterator(VERTEX_INDEX_CF);

        return new IteratorWithFnThenFilter<>(it,
            tuple2 -> vertexEncoder.getVertexID(getKeyFromKeyToVersion(tuple2.f0)),
            new DudupPredicate<>());
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(long version, IStatePushDown pushDown) {
        if (pushDown.getFilter() == null) {
            flush();
            byte[] prefix = getVersionPrefix(version);
            LmdbIterator it = lmdbClient.getIterator(VERTEX_CF, prefix);
            return new IteratorWithFnThenFilter<>(it,
                tuple2 -> vertexEncoder.getVertexID(getKeyFromVersionToKey(tuple2.f0)),
                new DudupPredicate<>());

        } else {
            return new IteratorWithFn<>(getVertexIterator(version, pushDown), IVertex::getId);
        }
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown) {
        flush();
        byte[] prefix = getVersionPrefix(version);
        LmdbIterator it = lmdbClient.getIterator(VERTEX_CF, prefix);
        return new VertexScanIterator<>(it, pushdown,
            (key, value) -> vertexEncoder.getVertex(getKeyFromVersionToKey(key), value));
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                               IStatePushDown pushdown) {
        return new KeysIterator<>(keys, (k, f) -> getVertex(version, k, f), pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        flush();
        byte[] prefix = getVersionPrefix(version);
        LmdbIterator it = lmdbClient.getIterator(EDGE_CF, prefix);
        return new EdgeScanIterator<>(it, pushdown,
            (key, value) -> edgeEncoder.getEdge(getKeyFromVersionToKey(key), value));
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                           IStatePushDown pushdown) {
        return new IteratorWithFlatFn<>(new KeysIterator<>(keys, (k, f) -> getEdges(version, k, f), pushdown), List::iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                                  IStatePushDown pushdown) {
        flush();
        return new OneDegreeGraphScanIterator<>(
            encoder.getKeyType(),
            getVertexIterator(version, pushdown),
            getEdgeIterator(version, pushdown),
            pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys,
                                                                                  IStatePushDown pushdown) {
        return new KeysIterator<>(keys, (k, f) -> getOneDegreeGraph(version, k, f), pushdown);
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        flush();
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            List<Long> list = new ArrayList<>();
            byte[] prefix = Bytes.concat(encoder.getKeyType().serialize(id), StateConfigKeys.DELIMITER);
            try (LmdbIterator it = this.lmdbClient.getIterator(VERTEX_INDEX_CF, prefix)) {
                while (it.hasNext()) {
                    Tuple<byte[], byte[]> pair = it.next();
                    list.add(getVersionFromKeyToVersion(pair.f0));
                }
            }
            return list;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public long getLatestVersion(K id, DataType dataType) {
        flush();
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            byte[] prefix = getKeyPrefix(id);
            try (LmdbIterator it = this.lmdbClient.getIterator(VERTEX_INDEX_CF, prefix)) {
                if (it.hasNext()) {
                    Tuple<byte[], byte[]> pair = it.next();
                    return getVersionFromKeyToVersion(pair.f0);
                }
            }
            return -1;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown,
                                                       DataType dataType) {
        List<Long> allVersions = getAllVersions(id, dataType);
        return getVersionData(id, allVersions, pushdown, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> versions,
                                                    IStatePushDown pushdown, DataType dataType) {
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            Map<Long, IVertex<K, VV>> map = new HashMap<>();
            for (long version : versions) {
                IVertex<K, VV> vertex = getVertex(version, id, pushdown);
                if (vertex != null) {
                    map.put(version, vertex);
                }
            }
            return map;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }


    @Override
    public LmdbClient getClient() {
        return lmdbClient;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    private long getVersionFromKeyToVersion(byte[] key) {
        byte[] bVersion = Arrays.copyOfRange(key, key.length - 8, key.length);
        return Long.MAX_VALUE - Longs.fromByteArray(bVersion);
    }

    protected byte[] getKeyFromKeyToVersion(byte[] key) {
        return Arrays.copyOf(key, key.length - VERTEX_INDEX_SUFFIX_SIZE);
    }

    protected byte[] getBinaryVersion(long version) {
        return Longs.toByteArray(Long.MAX_VALUE - version);
    }

    protected byte[] getKeyPrefix(K id) {
        return Bytes.concat(this.encoder.getKeyType().serialize(id), StateConfigKeys.DELIMITER);
    }

    protected byte[] getVersionPrefix(long version) {
        return Bytes.concat(getBinaryVersion(version), StateConfigKeys.DELIMITER);
    }

    protected byte[] getKeyFromVersionToKey(byte[] key) {
        return Arrays.copyOfRange(key, 10, key.length);
    }

    protected byte[] concat(byte[] a, byte[] b) {
        return Bytes.concat(a, StateConfigKeys.DELIMITER, b);
    }

    protected static class DudupPredicate<K> implements Predicate<K> {

        K last = null;

        @Override
        public boolean test(K k) {
            boolean res = k.equals(last);
            last = k;
            return !res;
        }
    }
}
