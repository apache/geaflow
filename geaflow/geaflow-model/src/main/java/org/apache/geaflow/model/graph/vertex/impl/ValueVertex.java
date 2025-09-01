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
import org.apache.geaflow.model.graph.vertex.IVertex;

public class ValueVertex<K, VV> implements IVertex<K, VV> {

    private K id;
    private VV value;

    public ValueVertex() {
    }

    public ValueVertex(K id) {
        this.id = id;
    }

    public ValueVertex(K id, VV value) {
        this.id = id;
        this.value = value;
    }

    @Override
    public K getId() {
        return this.id;
    }

    @Override
    public void setId(K id) {
        this.id = id;
    }

    @Override
    public VV getValue() {
        return this.value;
    }

    @Override
    public ValueVertex<K, VV> withValue(VV value) {
        this.value = value;
        return this;
    }

    @Override
    public ValueVertex<K, VV> withLabel(String label) {
        return new ValueLabelVertex<>(this.id, this.value, label);
    }

    @Override
    public ValueVertex<K, VV> withTime(long time) {
        return new ValueTimeVertex<>(this.id, this.value, time);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValueVertex<?, ?> idVertex = (ValueVertex<?, ?>) o;
        return Objects.equals(this.id, idVertex.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    @Override
    public String toString() {
        return String.format("ValueVertex(vertexId:%s, value:%s)", id, value);
    }

    @Override
    public int compareTo(Object o) {
        ValueVertex<K, VV> vertex = (ValueVertex<K, VV>) o;
        if (id instanceof Comparable) {
            return ((Comparable<K>) id).compareTo(vertex.getId());
        } else {
            return ((Integer) hashCode()).compareTo(vertex.hashCode());
        }
    }


}
