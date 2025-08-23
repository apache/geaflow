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
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;

public class IDLabelTimeVertex<K> extends IDVertex<K>
    implements IGraphElementWithLabelField, IGraphElementWithTimeField {

    private String label;
    private long time;

    public IDLabelTimeVertex() {
    }

    public IDLabelTimeVertex(K id) {
        super(id);
    }

    public IDLabelTimeVertex(K id, String label, long time) {
        super(id);
        this.label = label;
        this.time = time;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
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
    public ValueVertex<K, Object> withValue(Object value) {
        return new ValueLabelTimeVertex<>(this.getId(), value, this.label, this.time);
    }

    @Override
    public IDVertex<K> withLabel(String label) {
        this.label = label;
        return this;
    }

    @Override
    public IDVertex<K> withTime(long time) {
        this.time = time;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        IDLabelTimeVertex<?> that = (IDLabelTimeVertex<?>) o;
        return this.time == that.time && Objects.equals(this.label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.label, this.time);
    }

}
