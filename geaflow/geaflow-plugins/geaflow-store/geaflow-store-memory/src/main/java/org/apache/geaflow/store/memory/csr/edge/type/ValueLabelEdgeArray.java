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

package org.apache.geaflow.store.memory.csr.edge.type;

import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelEdge;

public class ValueLabelEdgeArray<K> extends ValueEdgeArray<K> {

    private String[] labels;

    public void init(Class<K> keyType, int capacity) {
        super.init(keyType, capacity);
        labels = new String[capacity];
    }

    @Override
    protected IEdge<K, Object> getEdge(K sid, int pos) {
        return new ValueLabelEdge<>(sid, getDstId(pos), getValue(pos),
            getDirection(pos), labels[pos]);
    }

    @Override
    public void drop() {
        super.drop();
        labels = null;
    }

    @Override
    public void set(int pos, IEdge<K, Object> edge) {
        super.set(pos, edge);
        labels[pos] = ((IGraphElementWithLabelField) edge).getLabel().intern();
    }

}
