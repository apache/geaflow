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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.ChainedCloseableIterator;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.TimeRange;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.FilterHelper;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.iterator.EdgeScanIterator;
import org.apache.geaflow.store.iterator.VertexScanIterator;
import org.apache.geaflow.store.rocksdb.RocksdbClient;
import org.apache.geaflow.store.rocksdb.RocksdbConfigKeys;
import org.apache.geaflow.store.rocksdb.iterator.RocksdbIterator;
import org.apache.geaflow.store.rocksdb.options.IRocksDBOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * GraphProxy which supported being partitioned by timestamp.
 */
public class SyncGraphDtPartitionProxy<K, VV, EV> extends SyncGraphRocksdbProxy<K, VV, EV> {

    // column family name -> column family handle
    private final Map<String, ColumnFamilyHandle> vertexHandleMap;
    private final Map<String, ColumnFamilyHandle> edgeHandleMap;
    private final Map<String, ColumnFamilyDescriptor> descriptorMap;
    private final IRocksDBOptions rocksDBOptions;


    // Dt config from RocksdbConfigKeys
    private final long startTimestamp;
    private final long cycle;

    public SyncGraphDtPartitionProxy(RocksdbClient rocksdbClient,
                                     IGraphKVEncoder<K, VV, EV> encoder, Configuration config) {
        super(rocksdbClient, encoder, config);
        this.vertexHandleMap = rocksdbClient.getVertexHandleMap();
        this.edgeHandleMap = rocksdbClient.getEdgeHandleMap();
        this.descriptorMap = rocksdbClient.getDescriptorMap();
        this.rocksDBOptions = rocksdbClient.getRocksDBOptions();

        this.startTimestamp = Long.parseLong(
            config.getString(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_DT_START));
        this.cycle = Long.parseLong(
            config.getString(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_DT_CYCLE));

    }

    public long getDt(long timestamp) {
        if (timestamp < startTimestamp) {
            throw new GeaflowRuntimeException(
                "timestamp " + timestamp + " is less than start " + startTimestamp);
        }
        long offset = (timestamp - startTimestamp) / cycle;
        return startTimestamp + offset * cycle;
    }

    private String getColumnFamilyNameByTimeStamp(long timestamp, boolean isVertex) {
        return (isVertex ? RocksdbConfigKeys.VERTEX_CF_PREFIX : RocksdbConfigKeys.EDGE_CF_PREFIX)
            + getDt(timestamp);
    }

    private List<String> getColumnFamilyNameListByTimeRange(TimeRange range, boolean isVertex) {
        long start = range.getStart();
        long end = range.getEnd();

        if (end < start) {
            throw new GeaflowRuntimeException(
                "timestamp end " + end + " is less than start " + start);
        }
        if (end < startTimestamp) {
            // 全部在start之前
            return new ArrayList<>();
        }

        long first = Math.max(start, startTimestamp);
        long firstDt = getDt(first);
        List<String> result = new ArrayList<>();
        for (long ts = firstDt; ts <= end; ts += cycle) {
            result.add(
                (isVertex ? RocksdbConfigKeys.VERTEX_CF_PREFIX : RocksdbConfigKeys.EDGE_CF_PREFIX)
                    + ts);
        }

        return result;
    }

    private ColumnFamilyHandle tryToGetOrCreateColumnFamilyHandle(
        Map<String, ColumnFamilyHandle> handleMap, IGraphElementWithTimeField element,
        boolean isVertex) {
        String cfName = getColumnFamilyNameByTimeStamp(element.getTime(), isVertex);

        return handleMap.computeIfAbsent(cfName, key -> {
            // Create ColumnFamilyDescriptor
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(cfName.getBytes(),
                rocksDBOptions.buildFamilyOptions());

            descriptorMap.put(cfName, descriptor);

            // Create ColumnFamilyHandle
            try {
                return rocksdbClient.getRocksdb().createColumnFamily(descriptor);
            } catch (RocksDBException e) {
                throw new GeaflowRuntimeException("Create column family " + cfName + " fail", e);
            }
        });
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        Tuple<byte[], byte[]> tuple = vertexEncoder.format(vertex);
        // Get the timestamp of the vertex
        // Create a new column family for a partitioned-dt never written
        ColumnFamilyHandle handle = tryToGetOrCreateColumnFamilyHandle(vertexHandleMap,
            (IGraphElementWithTimeField) vertex, true);

        this.rocksdbClient.write(handle, tuple.f0, tuple.f1);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        Tuple<byte[], byte[]> tuple = edgeEncoder.format(edge);
        // Get the timestamp of the edge
        // Create a new column family for a partitioned-dt never written
        ColumnFamilyHandle handle = tryToGetOrCreateColumnFamilyHandle(edgeHandleMap,
            (IGraphElementWithTimeField) edge, false);

        this.rocksdbClient.write(handle, tuple.f0, tuple.f1);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        IGraphFilter filter = null;
        TimeRange range = null;

        if (pushdown != null) {
            filter = (IGraphFilter) pushdown.getFilter();
            range = FilterHelper.parseDt(filter, true);
        }

        if (range == null) {
            for (ColumnFamilyHandle handle : vertexHandleMap.values()) {
                byte[] value = this.rocksdbClient.get(handle, key);
                if (value != null) {
                    IVertex<K, VV> vertex = vertexEncoder.getVertex(key, value);
                    if (filter == null || filter.filterVertex(vertex)) {
                        return vertex;
                    }
                }
            }
        } else {
            List<String> cfNames = getColumnFamilyNameListByTimeRange(range, true);
            for (String cfName : cfNames) {
                if (vertexHandleMap.containsKey(cfName)) {
                    byte[] value = this.rocksdbClient.get(vertexHandleMap.get(cfName), key);
                    if (value != null) {
                        IVertex<K, VV> vertex = vertexEncoder.getVertex(key, value);
                        if (filter.filterVertex(vertex)) {
                            return vertex;
                        }
                    }
                }
            }
        }

        return null;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        IGraphFilter filter = GraphFilter.of(pushdown.getFilter(), pushdown.getEdgeLimit());
        return getEdges(sid, filter);
    }

    protected List<IEdge<K, EV>> getEdges(K sid, IGraphFilter filter) {
        List<IEdge<K, EV>> list = new ArrayList<>();
        TimeRange range = FilterHelper.parseDt(filter, true);
        byte[] prefix = edgeEncoder.getScanBytes(sid);

        if (range == null) {
            for (ColumnFamilyHandle handle : edgeHandleMap.values()) {
                getEdgesFromSingleColumnFamily(handle, prefix, filter, list);
            }
        } else {
            List<String> cfNames = getColumnFamilyNameListByTimeRange(range, false);

            for (String cfName : cfNames) {
                if (edgeHandleMap.containsKey(cfName)) {
                    getEdgesFromSingleColumnFamily(edgeHandleMap.get(cfName), prefix, filter, list);
                }
            }
        }

        return list;
    }

    protected void getEdgesFromSingleColumnFamily(ColumnFamilyHandle handle, byte[] prefix,
                                                  IGraphFilter filter, List<IEdge<K, EV>> list) {
        try (RocksdbIterator it = new RocksdbIterator(this.rocksdbClient.getIterator(handle),
            prefix)) {
            getEdgesFromRocksDBIterator(list, it, filter);
        }
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        flush();

        List<RocksdbIterator> iterList = new ArrayList<>();
        for (ColumnFamilyHandle handle : vertexHandleMap.values()) {
            iterList.add(new RocksdbIterator(this.rocksdbClient.getIterator(handle)));
        }

        CloseableIterator<Tuple<byte[], byte[]>> it = new ChainedCloseableIterator(iterList);

        return buildVertexIDIteratorFromRocksDBIter(it);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        flush();

        return new VertexScanIterator<>(getVertexOrEdgeIterator(vertexHandleMap, pushdown, true),
            pushdown, vertexEncoder::getVertex);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        flush();
        return new EdgeScanIterator<>(getVertexOrEdgeIterator(edgeHandleMap, pushdown, false),
            pushdown, edgeEncoder::getEdge);
    }

    private CloseableIterator<Tuple<byte[], byte[]>> getVertexOrEdgeIterator(
        Map<String, ColumnFamilyHandle> handleMap, IStatePushDown pushdown, boolean isVertex) {

        IGraphFilter filter = (IGraphFilter) pushdown.getFilter();
        TimeRange range = FilterHelper.parseDt(filter, isVertex);
        List<RocksdbIterator> iterList = new ArrayList<>();

        if (range == null) {
            for (ColumnFamilyHandle handle : handleMap.values()) {
                iterList.add(new RocksdbIterator(this.rocksdbClient.getIterator(handle)));
            }
        } else {
            List<String> cfNames = getColumnFamilyNameListByTimeRange(range, isVertex);
            for (String cfName : cfNames) {

                if (handleMap.containsKey(cfName)) {
                    iterList.add(
                        new RocksdbIterator(this.rocksdbClient.getIterator(handleMap.get(cfName))));
                }
            }
        }

        return new ChainedCloseableIterator(iterList);
    }
}
