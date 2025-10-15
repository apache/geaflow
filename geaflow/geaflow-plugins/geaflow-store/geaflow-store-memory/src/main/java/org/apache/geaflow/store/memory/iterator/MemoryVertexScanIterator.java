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

package org.apache.geaflow.store.memory.iterator;

import java.util.Iterator;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.iterator.IVertexIterator;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;

public class MemoryVertexScanIterator<K, VV, EV> implements IVertexIterator<K, VV> {

    private final Iterator<IVertex<K, VV>> iterator;
    private final IGraphFilter filter;
    private IVertex<K, VV> nextValue;

    public MemoryVertexScanIterator(Iterator<IVertex<K, VV>> iterator, IGraphFilter filter) {
        this.iterator = iterator;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        while (iterator.hasNext()) {
            nextValue = iterator.next();
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

    }
}
