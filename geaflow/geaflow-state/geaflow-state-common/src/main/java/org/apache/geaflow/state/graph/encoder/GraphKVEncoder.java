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

package org.apache.geaflow.state.graph.encoder;

import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.state.schema.GraphDataSchema;

public class GraphKVEncoder<K, VV, EV> implements IGraphKVEncoder<K, VV, EV> {

    private GraphDataSchema graphDataSchema;
    private IType keyType;
    private IVertexKVEncoder<K, VV> vertexKVEncoder;
    private IEdgeKVEncoder<K, EV> edgeKVEncoder;

    public GraphKVEncoder() {

    }

    @Override
    public void init(GraphDataSchema graphDataSchema) {
        this.graphDataSchema = graphDataSchema;
        this.keyType = graphDataSchema.getKeyType();
        IBytesEncoder bytesEncoder = new DefaultBytesEncoder();
        this.vertexKVEncoder = this.graphDataSchema.isEmptyVertexProperty()
            ? new VertexKVEncoderWithoutValue<>(graphDataSchema, bytesEncoder)
            : new VertexKVEncoder<>(graphDataSchema, bytesEncoder);
        this.edgeKVEncoder = this.graphDataSchema.isEmptyEdgeProperty()
            ? new EdgeKVEncoderWithoutValue<>(graphDataSchema, bytesEncoder)
            : new EdgeKVEncoder<>(graphDataSchema, bytesEncoder);
    }

    @Override
    public IType<K> getKeyType() {
        return keyType;
    }

    @Override
    public IVertexKVEncoder<K, VV> getVertexEncoder() {
        return vertexKVEncoder;
    }

    @Override
    public IEdgeKVEncoder<K, EV> getEdgeEncoder() {
        return edgeKVEncoder;
    }
}
