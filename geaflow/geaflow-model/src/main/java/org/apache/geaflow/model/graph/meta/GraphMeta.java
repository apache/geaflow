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

package org.apache.geaflow.model.graph.meta;

import org.apache.geaflow.common.type.IType;

public class GraphMeta {

    private final IType keyType;
    private final IGraphElementMeta<?> vertexMeta;
    private final IGraphElementMeta<?> edgeMeta;

    public GraphMeta(GraphMetaType<?, ?, ?, ?, ?> graphMetaType) {
        this.keyType = graphMetaType.geKeyType();
        Class<?> vClass = graphMetaType.getVertexClass();
        Class<?> vvClass = graphMetaType.getVertexValueClass();
        Class<?> eClass = graphMetaType.getEdgeClass();
        Class<?> evClass = graphMetaType.getEdgeValueClass();
        this.vertexMeta = GraphElementMetas.getMeta(this.keyType, vClass, graphMetaType.getVertexConstruct(), vvClass);
        this.edgeMeta = GraphElementMetas.getMeta(this.keyType, eClass, graphMetaType.getEdgeConstruct(), evClass);
    }

    public IType getKeyType() {
        return this.keyType;
    }

    public IGraphElementMeta<?> getVertexMeta() {
        return this.vertexMeta;
    }

    public IGraphElementMeta<?> getEdgeMeta() {
        return this.edgeMeta;
    }

}
