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

package org.apache.geaflow.store.encoder;

import java.util.function.Function;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.state.schema.GraphDataSchema;

public abstract class BaseEncoder {

    protected final GraphDataSchema dataSchema;
    protected final IType keyType;
    protected final Function<Object, byte[]> valueSerializer;
    protected final Function<byte[], Object> valueDeserializer;
    protected final boolean emptyProperty;

    protected BaseEncoder(GraphDataSchema dataSchema) {
        this.dataSchema = dataSchema;
        this.keyType = dataSchema.getKeyType();
        this.valueSerializer = initValueSerializer(dataSchema);
        this.valueDeserializer = initValueDeserializer(dataSchema);
        this.emptyProperty = initEmptyProperty(dataSchema);
    }

    protected abstract Function<Object, byte[]> initValueSerializer(GraphDataSchema dataSchema);

    protected abstract Function<byte[], Object> initValueDeserializer(GraphDataSchema dataSchema);

    protected abstract boolean initEmptyProperty(GraphDataSchema dataSchema);

    protected GraphDataSchema getDataSchema() {
        return dataSchema;
    }

    protected Function<Object, byte[]> getValueSerializer() {
        return valueSerializer;
    }

    protected Function<byte[], Object> getValueDeserializer() {
        return valueDeserializer;
    }

    protected boolean isEmptyProperty() {
        return emptyProperty;
    }
}
