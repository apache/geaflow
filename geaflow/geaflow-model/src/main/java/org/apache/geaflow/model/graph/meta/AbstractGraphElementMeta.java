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

import java.util.function.Supplier;
import org.apache.geaflow.common.schema.ISchema;
import org.apache.geaflow.common.type.IType;

public abstract class AbstractGraphElementMeta<ELEMENT> implements IGraphElementMeta<ELEMENT> {

    private final byte graphElementId;
    private final Class<ELEMENT> elementClass;
    private final Supplier<ELEMENT> elementConstruct;
    private final Class<?> propertyClass;
    private final ISchema primitiveSchema;

    public AbstractGraphElementMeta(byte graphElementId,
                                    IType keyType,
                                    Class<ELEMENT> elementClass,
                                    Supplier<ELEMENT> elementConstruct,
                                    Class<?> propertyClass) {
        this.graphElementId = graphElementId;
        this.elementClass = elementClass;
        this.elementConstruct = elementConstruct;
        this.propertyClass = propertyClass;
        this.primitiveSchema = GraphElementSchemaFactory.newSchema(keyType, elementClass);
    }

    @Override
    public byte getGraphElementId() {
        return this.graphElementId;
    }

    @Override
    public Class<ELEMENT> getGraphElementClass() {
        return this.elementClass;
    }

    @Override
    public Supplier<ELEMENT> getGraphElementConstruct() {
        return elementConstruct;
    }

    @Override
    public ISchema getGraphMeta() {
        return this.primitiveSchema;
    }

    @Override
    public Class<?> getPropertyClass() {
        return this.propertyClass;
    }

}
