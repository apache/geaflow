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

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.schema.GraphDataSchema;

public class VertexKVEncoderWithoutValue<K, VV> implements IVertexKVEncoder<K, VV> {

    protected final GraphDataSchema graphDataSchema;
    protected final List<VertexAtom> vertexSchema;
    protected final IType keyType;
    protected final IBytesEncoder bytesEncoder;

    public VertexKVEncoderWithoutValue(GraphDataSchema graphDataSchema, IBytesEncoder bytesEncoder) {
        this.graphDataSchema = graphDataSchema;
        this.vertexSchema = graphDataSchema.getVertexAtoms();
        this.keyType = graphDataSchema.getKeyType();
        this.bytesEncoder = bytesEncoder;
    }

    @Override
    public Tuple<byte[], byte[]> format(IVertex<K, VV> vertex) {
        byte[] a = keyType.serialize(vertex.getId());

        List<byte[]> bytes = new ArrayList<>();
        for (int i = 1; i < vertexSchema.size(); i++) {
            bytes.add(vertexSchema.get(i).getBinaryValue(vertex, graphDataSchema));
        }

        return new Tuple<>(a, bytesEncoder.combine(bytes));
    }

    @Override
    public IVertex<K, VV> getVertex(byte[] key, byte[] value) {
        IVertex vertex = this.graphDataSchema.getVertexConsFun().get();
        vertex.setId(keyType.deserialize(key));
        List<byte[]> values = bytesEncoder.split(value);
        if (values == null) {
            IBytesEncoder encoder = BytesEncoderRepo.get(
                bytesEncoder.parseMagicNumber(key[value.length - 1]));
            values = encoder.split(value);
        }
        int i = 1;
        for (; i < vertexSchema.size(); i++) {
            vertexSchema.get(i).setBinaryValue(vertex, values.get(i - 1), graphDataSchema);
        }
        return vertex;
    }

    @Override
    public K getVertexID(byte[] key) {
        return (K) keyType.deserialize(key);
    }

    @Override
    public IBytesEncoder getBytesEncoder() {
        return this.bytesEncoder;
    }
}
