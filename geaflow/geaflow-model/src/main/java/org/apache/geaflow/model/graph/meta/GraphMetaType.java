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

import java.io.Serializable;
import java.util.function.Supplier;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class GraphMetaType<K, VV, EV, V extends IVertex<K, VV>, E extends IEdge<K, EV>> implements Serializable {

    private IType<K> keyType;
    private Class<V> vertexClass;
    private Supplier<V> vertexConstruct;
    private Class<VV> vertexValueClass;
    private Class<E> edgeClass;
    private Supplier<E> edgeConstruct;
    private Class<EV> edgeValueClass;

    public GraphMetaType() {
    }

    public GraphMetaType(IType<K> keyType,
                         Class<V> vertexClass,
                         Supplier<V> vertexConstruct,
                         Class<VV> vertexValueClass,
                         Class<E> edgeClass,
                         Supplier<E> edgeConstruct,
                         Class<EV> edgeValueClass) {
        this.keyType = keyType;
        this.vertexClass = vertexClass;
        this.vertexConstruct = vertexConstruct;
        this.vertexValueClass = vertexValueClass;
        this.edgeClass = edgeClass;
        this.edgeConstruct = edgeConstruct;
        this.edgeValueClass = edgeValueClass;
    }

    public GraphMetaType(IType<K> keyType,
                         Class<V> vertexClass,
                         Class<VV> vertexValueClass,
                         Class<E> edgeClass,
                         Class<EV> edgeValueClass) {
        this(keyType, vertexClass,
            new DefaultObjectConstruct<>(vertexClass),
            vertexValueClass, edgeClass,
            new DefaultObjectConstruct<>(edgeClass),
            edgeValueClass);
    }

    public IType geKeyType() {
        return this.keyType;
    }

    public void setKeyType(IType keyType) {
        this.keyType = keyType;
    }

    public Class<V> getVertexClass() {
        return this.vertexClass;
    }

    public void setVertexClass(Class<V> vertexClass) {
        this.vertexClass = vertexClass;
    }

    public Class<VV> getVertexValueClass() {
        return this.vertexValueClass;
    }

    public void setVertexValueClass(Class<VV> vertexValueClass) {
        this.vertexValueClass = vertexValueClass;
    }

    public Class<E> getEdgeClass() {
        return this.edgeClass;
    }

    public void setEdgeClass(Class<E> edgeClass) {
        this.edgeClass = edgeClass;
    }

    public Class<EV> getEdgeValueClass() {
        return this.edgeValueClass;
    }

    public void setEdgeValueClass(Class<EV> edgeValueClass) {
        this.edgeValueClass = edgeValueClass;
    }

    public Supplier<V> getVertexConstruct() {
        return vertexConstruct;
    }

    public void setVertexConstruct(Supplier<V> vertexConstruct) {
        this.vertexConstruct = vertexConstruct;
    }

    public Supplier<E> getEdgeConstruct() {
        return edgeConstruct;
    }

    public void setEdgeConstruct(Supplier<E> edgeConstruct) {
        this.edgeConstruct = edgeConstruct;
    }

    @Override
    public String toString() {
        return "GraphMetaType{"
            + "keyType=" + keyType
            + ", vertexClass=" + vertexClass
            + ", vertexConstruct=" + vertexConstruct
            + ", vertexValueClass=" + vertexValueClass
            + ", edgeClass=" + edgeClass
            + ", edgeConstruct=" + edgeConstruct
            + ", edgeValueClass=" + edgeValueClass
            + '}';
    }

    private static class DefaultObjectConstruct<C> implements Supplier<C> {

        private final Class<C> clazz;

        public DefaultObjectConstruct(Class<C> clazz) {
            this.clazz = clazz;
        }

        @Override
        public C get() {
            try {
                return clazz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new GeaflowRuntimeException("Error in create instance for class: " + clazz, e);
            }
        }
    }
}
