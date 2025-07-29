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

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.store.paimon.PaimonTableRWHandle;
import org.apache.geaflow.store.paimon.iterator.PaimonIterator;
import org.apache.geaflow.store.paimon.predicate.BytesStartsWith;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.RowType;

public class PaimonGraphRWProxy<K, VV, EV> extends PaimonBaseGraphProxy<K, VV, EV> {

    public PaimonGraphRWProxy(PaimonTableRWHandle vertexHandle, PaimonTableRWHandle edgeHandle,
                              int[] projection, IGraphKVEncoder<K, VV, EV> encoder) {
        super(vertexHandle, edgeHandle, projection, encoder);
    }

    @Override
    public void archive(long checkpointId) {
        this.lastCheckpointId = checkpointId;
        this.vertexHandle.commit(lastCheckpointId);
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
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        RowType rowType = vertexHandle.getTable().rowType();
        Predicate predicate = new LeafPredicate(Equal.INSTANCE, rowType.getTypeAt(0), 0,
            rowType.getField(0).name(), singletonList(key));
        RecordReaderIterator<InternalRow> iterator = this.vertexHandle.getIterator(predicate, null,
            projection);
        try (PaimonIterator paimonIterator = new PaimonIterator(iterator)) {
            if (paimonIterator.hasNext()) {
                Tuple<byte[], byte[]> row = paimonIterator.next();
                IVertex<K, VV> vertex = encoder.getVertexEncoder()
                    .getVertex(row.getF0(), row.getF1());
                if (pushdown == null || ((IGraphFilter) pushdown.getFilter()).filterVertex(
                    vertex)) {
                    return vertex;
                }
            }
            return null;
        }
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        byte[] prefixBytes = encoder.getEdgeEncoder().getScanBytes(sid);
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
                IEdge<K, EV> edge = encoder.getEdgeEncoder().getEdge(row.getF0(), row.getF1());
                if (graphFilter.filterEdge(edge)) {
                    edges.add(edge);
                }
                if (graphFilter.dropAllRemaining()) {
                    break;
                }
            }
            return edges;
        }
    }
}
