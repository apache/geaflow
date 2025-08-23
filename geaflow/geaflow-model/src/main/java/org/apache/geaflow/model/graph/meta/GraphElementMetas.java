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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

@SuppressWarnings("ALL")
public class GraphElementMetas {

    private static final ThreadLocal<Map<Class<?>, IGraphElementMeta<?>>> META_CACHE =
        ThreadLocal.withInitial(HashMap::new);

    public static IGraphElementMeta<?> getMeta(IType keyType, Class<?> elementClass, Supplier<?> elementConstruct,
                                               Class<?> propertyClass) {
        Map<Class<?>, IGraphElementMeta<?>> map = META_CACHE.get();
        return map.computeIfAbsent(elementClass,
            k -> newGraphElementMeta(keyType, elementClass, elementConstruct, propertyClass));
    }

    public static void clearCache() {
        META_CACHE.remove();
    }

    private static IGraphElementMeta<?> newGraphElementMeta(IType keyType,
                                                            Class<?> elementClass,
                                                            Supplier<?> elementConstruct,
                                                            Class<?> propertyClass) {
        GraphElementFlag flag = GraphElementFlag.build(elementClass);
        byte graphElementId = flag.toGraphElementId();
        if (flag.isLabeled()) {
            return new LabelElementMeta(graphElementId, keyType, elementClass, elementConstruct, propertyClass);
        } else if (flag.isTimed()) {
            return new TimeElementMeta(graphElementId, keyType, elementClass, elementConstruct, propertyClass);
        } else if (flag.isLabeledAndTimed()) {
            return new LabelTimeElementMeta(graphElementId, keyType, elementClass, elementConstruct, propertyClass);
        } else {
            return new IdElementMeta(graphElementId, keyType, elementClass, elementConstruct, propertyClass);
        }
    }

    public static class GraphElementFlag {

        private static final byte MASK_LABEL = 1 << 0;
        private static final byte MASK_TIME = 1 << 1;

        private byte flag = 0;

        public void markLabeled() {
            this.flag |= MASK_LABEL;
        }

        public void markTimed() {
            this.flag |= MASK_TIME;
        }

        public boolean isLabeled() {
            return (this.flag & MASK_LABEL) > 0 && (this.flag & MASK_TIME) == 0;
        }

        public boolean isTimed() {
            return (this.flag & MASK_LABEL) == 0 && (this.flag & MASK_TIME) > 0;
        }

        public boolean isLabeledAndTimed() {
            return (this.flag & MASK_LABEL) > 0 && (this.flag & MASK_TIME) > 0;
        }

        public byte toGraphElementId() {
            return this.flag;
        }

        public static GraphElementFlag build(Class<?> elementClass) {
            if (!IVertex.class.isAssignableFrom(elementClass) && !IEdge.class.isAssignableFrom(elementClass)) {
                String msg = "unrecognized graph element class: " + elementClass.getCanonicalName();
                throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
            }
            GraphElementFlag tag = new GraphElementFlag();
            if (IGraphElementWithLabelField.class.isAssignableFrom(elementClass)) {
                tag.markLabeled();
            }
            if (IGraphElementWithTimeField.class.isAssignableFrom(elementClass)) {
                tag.markTimed();
            }
            return tag;
        }

    }

    public static class IdElementMeta<ELEMENT> extends AbstractGraphElementMeta<ELEMENT> {

        public IdElementMeta(byte graphElementId,
                             IType keyType,
                             Class<ELEMENT> elementClass,
                             Supplier<ELEMENT> elementConstruct,
                             Class<?> propertyClass) {
            super(graphElementId, keyType, elementClass, elementConstruct, propertyClass);
        }

        @Override
        public IGraphFieldSerializer<ELEMENT> getGraphFieldSerializer() {
            return GraphFieldSerializers.IdFieldSerializer.INSTANCE;
        }

    }

    public static class LabelElementMeta<ELEMENT extends IGraphElementWithLabelField>
        extends AbstractGraphElementMeta<ELEMENT> {

        public LabelElementMeta(byte graphElementId,
                                IType keyType,
                                Class<ELEMENT> elementClass,
                                Supplier<ELEMENT> elementConstruct,
                                Class<?> propertyClass) {
            super(graphElementId, keyType, elementClass, elementConstruct, propertyClass);
        }

        @Override
        public IGraphFieldSerializer<ELEMENT> getGraphFieldSerializer() {
            return GraphFieldSerializers.LabelFieldSerializer.INSTANCE;
        }

    }

    public static class TimeElementMeta<ELEMENT extends IGraphElementWithTimeField>
        extends AbstractGraphElementMeta<ELEMENT> {

        public TimeElementMeta(byte graphElementId,
                               IType keyType,
                               Class<?> elementClass,
                               Supplier<ELEMENT> elementConstruct,
                               Class<?> propertyClass) {
            super(graphElementId, keyType, (Class<ELEMENT>) elementClass, elementConstruct, propertyClass);
        }

        @Override
        public IGraphFieldSerializer<ELEMENT> getGraphFieldSerializer() {
            return GraphFieldSerializers.TimeFieldSerializer.INSTANCE;
        }

    }

    public static class LabelTimeElementMeta<ELEMENT extends IGraphElementWithLabelField & IGraphElementWithTimeField>
        extends AbstractGraphElementMeta<ELEMENT> {

        public LabelTimeElementMeta(byte graphElementId,
                                    IType keyType,
                                    Class<ELEMENT> elementClass,
                                    Supplier<ELEMENT> elementConstruct,
                                    Class<?> propertyClass) {
            super(graphElementId, keyType, (Class<ELEMENT>) elementClass, elementConstruct, propertyClass);
        }

        @Override
        public IGraphFieldSerializer<ELEMENT> getGraphFieldSerializer() {
            return GraphFieldSerializers.LabelTimeFieldSerializer.INSTANCE;
        }

    }

}
