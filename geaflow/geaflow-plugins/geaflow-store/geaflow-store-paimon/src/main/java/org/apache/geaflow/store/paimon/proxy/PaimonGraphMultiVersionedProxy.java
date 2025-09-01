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

package org.apache.geaflow.store.paimon.proxy;

import static java.util.Collections.singletonList;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.iterator.EdgeScanIterator;
import org.apache.geaflow.store.iterator.KeysIterator;
import org.apache.geaflow.store.iterator.OneDegreeGraphScanIterator;
import org.apache.geaflow.store.iterator.VertexScanIterator;
import org.apache.geaflow.store.paimon.PaimonTableRWHandle;
import org.apache.geaflow.store.paimon.iterator.PaimonIterator;
import org.apache.geaflow.store.paimon.predicate.BytesStartsWith;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.RowType;

public class PaimonGraphMultiVersionedProxy<K, VV, EV> implements
    IGraphMultiVersionedPaimonProxy<K, VV, EV> {

    private static final int VERSION_BYTES_SIZE = Long.BYTES;
    private static final int VERTEX_INDEX_SUFFIX_SIZE =
        VERSION_BYTES_SIZE + StateConfigKeys.DELIMITER.length;
    protected static final byte[] EMPTY_BYTES = new byte[0];
    protected final Configuration config;
    protected IGraphKVEncoder<K, VV, EV> encoder;
    protected IEdgeKVEncoder<K, EV> edgeEncoder;
    protected IVertexKVEncoder<K, VV> vertexEncoder;

    protected PaimonTableRWHandle vertexHandle;
    protected PaimonTableRWHandle vertexIndexHandle;
    protected PaimonTableRWHandle edgeHandle;

    protected int[] projection;
    protected long lastCheckpointId;

    public PaimonGraphMultiVersionedProxy(PaimonTableRWHandle vertexHandle,
                                          PaimonTableRWHandle vertexIndexHandle,
                                          PaimonTableRWHandle edgeHandle, int[] projection,
                                          IGraphKVEncoder<K, VV, EV> encoder,
                                          Configuration config) {
        this.vertexHandle = vertexHandle;
        this.vertexIndexHandle = vertexIndexHandle;
        this.edgeHandle = edgeHandle;
        this.projection = projection;
        this.encoder = encoder;
        this.vertexEncoder = encoder.getVertexEncoder();
        this.edgeEncoder = encoder.getEdgeEncoder();
        this.config = config;
        this.lastCheckpointId = 0;
    }

    @Override
    public void archive(long checkpointId) {
        this.lastCheckpointId = checkpointId;
        this.vertexHandle.commit(lastCheckpointId);
        this.vertexIndexHandle.commit(lastCheckpointId);
        this.edgeHandle.commit(lastCheckpointId);
    }

    @Override
    public void recover(long checkpointId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long recoverLatest() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        Tuple<byte[], byte[]> tuple = edgeEncoder.format(edge);
        byte[] bVersion = getBinaryVersion(version);
        GenericRow record = GenericRow.of(concat(bVersion, tuple.f0), tuple.f1);
        this.edgeHandle.write(record, 0);
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        Tuple<byte[], byte[]> tuple = vertexEncoder.format(vertex);
        byte[] bVersion = getBinaryVersion(version);
        GenericRow record = GenericRow.of(concat(bVersion, tuple.f0), tuple.f1);
        GenericRow index = GenericRow.of(concat(tuple.f0, bVersion), EMPTY_BYTES);
        this.vertexHandle.write(record, 0);
        this.vertexIndexHandle.write(index, 0);
    }

    @Override
    public IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        byte[] bVersion = getBinaryVersion(version);
        RowType rowType = vertexHandle.getTable().rowType();
        Predicate predicate = new LeafPredicate(Equal.INSTANCE, rowType.getTypeAt(0), 0,
            rowType.getField(0).name(), singletonList(concat(bVersion, key)));
        RecordReaderIterator<InternalRow> iterator = this.vertexHandle.getIterator(predicate, null,
            projection);
        try (PaimonIterator paimonIterator = new PaimonIterator(iterator)) {
            if (paimonIterator.hasNext()) {
                Tuple<byte[], byte[]> row = paimonIterator.next();
                IVertex<K, VV> vertex = vertexEncoder.getVertex(key, row.getF1());
                if (pushdown == null || ((IGraphFilter) pushdown.getFilter()).filterVertex(
                    vertex)) {
                    return vertex;
                }
            }
            return null;
        }
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown) {
        byte[] bVersion = getBinaryVersion(version);
        byte[] prefixBytes = concat(bVersion, edgeEncoder.getScanBytes(sid));
        RowType rowType = edgeHandle.getTable().rowType();
        Predicate predicate = new LeafPredicate(BytesStartsWith.INSTANCE, rowType.getTypeAt(0), 0,
            rowType.getField(0).name(), singletonList(prefixBytes));
        RecordReaderIterator<InternalRow> iterator = this.edgeHandle.getIterator(predicate, null,
            projection);
        List<IEdge<K, EV>> edges = new ArrayList<>();
        try (PaimonIterator paimonIterator = new PaimonIterator(iterator)) {
            IGraphFilter graphFilter = GraphFilter.of(pushdown.getFilter(),
                pushdown.getEdgeLimit());
            while (paimonIterator.hasNext()) {
                Tuple<byte[], byte[]> row = paimonIterator.next();
                IEdge<K, EV> edge = edgeEncoder.getEdge(getKeyFromVersionToKey(row.f0), row.f1);
                if (graphFilter.filterEdge(edge)) {
                    edges.add(edge);
                }
            }
            return edges;
        }
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
        flush();

        RecordReaderIterator<InternalRow> iterator = this.vertexIndexHandle.getIterator(null, null,
            projection);

        return new IteratorWithFnThenFilter<>(iterator,
            tuple2 -> vertexEncoder.getVertexID(getKeyFromKeyToVersion(tuple2.getBinary(0))),
            new DeDupPredicate<>());
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(long version, IStatePushDown pushdown) {
        if (pushdown.getFilter() == null) {
            flush();
            byte[] prefix = getVersionPrefix(version);
            RowType rowType = vertexIndexHandle.getTable().rowType();
            Predicate predicate = new LeafPredicate(BytesStartsWith.INSTANCE, rowType.getTypeAt(0),
                0, rowType.getField(0).name(), singletonList(prefix));
            RecordReaderIterator<InternalRow> iterator = this.vertexIndexHandle.getIterator(
                predicate, null, projection);
            return new IteratorWithFnThenFilter<>(iterator,
                tuple2 -> vertexEncoder.getVertexID(getKeyFromVersionToKey(tuple2.getBinary(0))),
                new DeDupPredicate<>());

        } else {
            return new IteratorWithFn<>(getVertexIterator(version, pushdown), IVertex::getId);
        }
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version,
                                                               IStatePushDown pushdown) {
        flush();
        byte[] prefix = getVersionPrefix(version);
        RowType rowType = vertexHandle.getTable().rowType();
        Predicate predicate = new LeafPredicate(BytesStartsWith.INSTANCE, rowType.getTypeAt(0), 0,
            rowType.getField(0).name(), singletonList(prefix));
        RecordReaderIterator<InternalRow> iterator = this.vertexHandle.getIterator(predicate, null,
            projection);
        PaimonIterator it = new PaimonIterator(iterator);
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
        RowType rowType = edgeHandle.getTable().rowType();
        Predicate predicate = new LeafPredicate(BytesStartsWith.INSTANCE, rowType.getTypeAt(0), 0,
            rowType.getField(0).name(), singletonList(prefix));
        RecordReaderIterator<InternalRow> iterator = this.edgeHandle.getIterator(predicate, null,
            projection);
        PaimonIterator it = new PaimonIterator(iterator);
        return new EdgeScanIterator<>(it, pushdown,
            (key, value) -> edgeEncoder.getEdge(getKeyFromVersionToKey(key), value));
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                           IStatePushDown pushdown) {
        return new IteratorWithFlatFn<>(
            new KeysIterator<>(keys, (k, f) -> getEdges(version, k, f), pushdown), List::iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                                  IStatePushDown pushdown) {
        flush();
        return new OneDegreeGraphScanIterator<>(encoder.getKeyType(),
            getVertexIterator(version, pushdown), getEdgeIterator(version, pushdown), pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                                  List<K> keys,
                                                                                  IStatePushDown pushdown) {
        return new KeysIterator<>(keys, (k, f) -> getOneDegreeGraph(version, k, f), pushdown);
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        flush();
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            List<Long> list = new ArrayList<>();
            byte[] prefix = Bytes.concat(encoder.getKeyType().serialize(id),
                StateConfigKeys.DELIMITER);
            RowType rowType = vertexIndexHandle.getTable().rowType();
            Predicate predicate = new LeafPredicate(BytesStartsWith.INSTANCE, rowType.getTypeAt(0),
                0, rowType.getField(0).name(), singletonList(prefix));
            RecordReaderIterator<InternalRow> iterator = this.vertexIndexHandle.getIterator(
                predicate, null, projection);
            try (PaimonIterator it = new PaimonIterator(iterator)) {
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
            RowType rowType = vertexIndexHandle.getTable().rowType();
            Predicate predicate = new LeafPredicate(BytesStartsWith.INSTANCE, rowType.getTypeAt(0),
                0, rowType.getField(0).name(), singletonList(prefix));
            RecordReaderIterator<InternalRow> iterator = this.vertexIndexHandle.getIterator(
                predicate, null, projection);
            try (PaimonIterator it = new PaimonIterator(iterator)) {
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

    protected static class DeDupPredicate<K> implements java.util.function.Predicate<K> {

        K last = null;

        @Override
        public boolean test(K k) {
            boolean res = k.equals(last);
            last = k;
            return !res;
        }
    }
}
