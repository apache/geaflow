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

package org.apache.geaflow.model.graph.vertex.impl;

import java.util.Objects;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;

public class ValueTimeVertex<K, VV> extends ValueVertex<K, VV> implements IGraphElementWithTimeField {

    private long time;

    public ValueTimeVertex() {
    }

    public ValueTimeVertex(K id) {
        super(id);
    }

    public ValueTimeVertex(K id, VV value, long time) {
        super(id, value);
        this.time = time;
    }

    @Override
    public long getTime() {
        return this.time;
    }

    @Override
    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public ValueVertex<K, VV> withLabel(String label) {
        return new ValueLabelTimeVertex<>(this.getId(), this.getValue(), label, this.time);
    }

    @Override
    public ValueVertex<K, VV> withTime(long time) {
        this.time = time;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        ValueTimeVertex<?, ?> that = (ValueTimeVertex<?, ?>) o;
        return this.time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.time);
    }

}
