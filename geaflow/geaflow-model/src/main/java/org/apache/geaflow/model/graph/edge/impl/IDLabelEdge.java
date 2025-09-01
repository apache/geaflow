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
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

public class IDLabelEdge<K> extends IDEdge<K> implements IGraphElementWithLabelField {

    private String label;

    public IDLabelEdge() {
    }

    public IDLabelEdge(K src, K target) {
        this(src, target, null);
    }

    public IDLabelEdge(K src, K target, String label) {
        this(src, target, EdgeDirection.OUT, label);
    }

    public IDLabelEdge(K srcId, K targetId, EdgeDirection edgeDirection, String label) {
        super(srcId, targetId, edgeDirection);
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
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        IDLabelEdge<?> that = (IDLabelEdge<?>) o;
        return Objects.equals(this.label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.label);
    }

}
