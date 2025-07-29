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

package org.apache.geaflow.store.iterator;

import java.util.function.BiFunction;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.iterator.IVertexIterator;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;

public class VertexScanIterator<K, VV, EV> implements IVertexIterator<K, VV> {

    private final CloseableIterator<Tuple<byte[], byte[]>> iterator;
    private final IGraphFilter filter;
    private final BiFunction<byte[], byte[], IVertex<K, VV>> vertexDecoder;
    private boolean isClosed = false;
    private IVertex<K, VV> nextValue;

    public VertexScanIterator(CloseableIterator<Tuple<byte[], byte[]>> iterator,
                              IStatePushDown pushdown,
                              BiFunction<byte[], byte[], IVertex<K, VV>> decoderFun) {
        this.vertexDecoder = decoderFun;
        this.iterator = iterator;
        this.filter = (IGraphFilter) pushdown.getFilter();
    }

    @Override
    public boolean hasNext() {
        if (isClosed) {
            return false;
        }
        while (iterator.hasNext()) {
            Tuple<byte[], byte[]> pair = iterator.next();
            nextValue = vertexDecoder.apply(pair.f0, pair.f1);

            if (!filter.filterVertex(nextValue)) {
                continue;
            }
            return true;
        }

        return false;
    }

    @Override
    public IVertex<K, VV> next() {
        return nextValue;
    }

    @Override
    public void close() {
        this.iterator.close();
    }
}
