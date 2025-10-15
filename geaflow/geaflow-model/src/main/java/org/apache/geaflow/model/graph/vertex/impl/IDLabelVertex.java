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

public class IDLabelVertex<K> extends IDVertex<K> implements IGraphElementWithLabelField {

    private String label;

    public IDLabelVertex() {
    }

    public IDLabelVertex(K id) {
        super(id);
    }

    public IDLabelVertex(K id, String label) {
        super(id);
        this.label = label;
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
    public ValueVertex<K, Object> withValue(Object value) {
        return new ValueLabelVertex<>(this.getId(), value, this.label);
    }

    @Override
    public IDVertex<K> withLabel(String label) {
        this.label = label;
        return this;
    }

    @Override
    public IDVertex<K> withTime(long time) {
        return new IDLabelTimeVertex<>(this.getId(), this.label, time);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        IDLabelVertex<?> that = (IDLabelVertex<?>) o;
        return Objects.equals(this.label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.label);
    }

}
