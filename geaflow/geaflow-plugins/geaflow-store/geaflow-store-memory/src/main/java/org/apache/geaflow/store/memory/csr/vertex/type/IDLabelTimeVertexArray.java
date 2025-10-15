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

package org.apache.geaflow.store.memory.csr.vertex.type;

import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.IDLabelTimeVertex;

public class IDLabelTimeVertexArray<K> extends IDVertexArray<K> {

    private String[] labels;
    private long[] times;

    public void init(int capacity) {
        super.init(capacity);
        labels = new String[capacity];
        times = new long[capacity];
    }

    @Override
    public IVertex<K, Object> getVertex(K key, int pos) {
        return containsVertex(pos) ? new IDLabelTimeVertex<>(key, labels[pos], times[pos]) : null;
    }

    @Override
    public void set(int pos, IVertex<K, Object> vertex) {
        super.set(pos, vertex);
        if (vertex != null) {
            labels[pos] = ((IGraphElementWithLabelField) vertex).getLabel().intern();
            times[pos] = ((IGraphElementWithTimeField) vertex).getTime();
        }
    }

    @Override
    public void drop() {
        super.drop();
        labels = null;
        times = null;
    }

}
