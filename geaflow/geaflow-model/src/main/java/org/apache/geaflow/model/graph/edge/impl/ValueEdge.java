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

package org.apache.geaflow.model.graph.edge.impl;

import java.util.Objects;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;

public class ValueEdge<K, EV> implements IEdge<K, EV> {

    private K srcId;
    private K targetId;
    private EdgeDirection direction;
    private EV value;

    public ValueEdge() {
    }

    public ValueEdge(K srcId, K targetId) {
        this(srcId, targetId, null);
    }

    public ValueEdge(K srcId, K targetId, EV value) {
        this(srcId, targetId, value, EdgeDirection.OUT);
    }

    public ValueEdge(K srcId, K targetId, EV value, EdgeDirection edgeDirection) {
        this.srcId = srcId;
        this.targetId = targetId;
        this.direction = edgeDirection;
        this.value = value;
    }

    @Override
    public K getSrcId() {
        return this.srcId;
    }

    @Override
    public void setSrcId(K srcId) {
        this.srcId = srcId;
    }

    @Override
    public K getTargetId() {
        return this.targetId;
    }

    @Override
    public void setTargetId(K targetId) {
        this.targetId = targetId;
    }

    @Override
    public IEdge<K, EV> reverse() {
        return new ValueEdge<>(this.targetId, this.srcId, this.value);
    }

    @Override
    public EV getValue() {
        return this.value;
    }

    @Override
    public EdgeDirection getDirect() {
        return this.direction;
    }

    @Override
    public IEdge<K, EV> withValue(EV value) {
        this.value = value;
        return this;
    }

    @Override
    public void setDirect(EdgeDirection direction) {
        this.direction = direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        ValueEdge<?, ?> that = (ValueEdge<?, ?>) o;
        return Objects.equals(this.srcId, that.srcId)
            && Objects.equals(this.targetId, that.targetId)
            && this.direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.srcId, this.targetId, this.direction);
    }

    @Override
    public String toString() {
        return "ValueEdge{" + "srcId=" + srcId + ", targetId=" + targetId + ", direction="
            + direction + ", value=" + value + '}';
    }
}
