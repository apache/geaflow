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

import com.google.common.primitives.Bytes;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.schema.GraphDataSchema;

public class EdgeKVEncoderWithoutValue<K, EV> implements IEdgeKVEncoder<K, EV> {

    protected static final byte[] EMPTY_BYTES = new byte[0];
    protected final GraphDataSchema graphDataSchema;
    protected final List<EdgeAtom> edgeSchema;
    protected final IType keyType;
    protected final IBytesEncoder bytesEncoder;

    public EdgeKVEncoderWithoutValue(GraphDataSchema graphDataSchema, IBytesEncoder bytesEncoder) {
        this.graphDataSchema = graphDataSchema;
        this.edgeSchema = graphDataSchema.getEdgeAtoms();
        this.keyType = graphDataSchema.getKeyType();
        this.bytesEncoder = bytesEncoder;
    }

    @Override
    public byte[] getScanBytes(K key) {
        return Bytes.concat(keyType.serialize(key), StateConfigKeys.DELIMITER);
    }

    @Override
    public Tuple<byte[], byte[]> format(IEdge<K, EV> edge) {
        List<byte[]> list = new ArrayList<>(edgeSchema.size());
        for (int i = 0; i < edgeSchema.size(); i++) {
            list.add(edgeSchema.get(i).getBinaryValue(edge, graphDataSchema));
        }
        byte[] a = bytesEncoder.combine(list, StateConfigKeys.DELIMITER);
        return new Tuple<>(a, EMPTY_BYTES);
    }

    @Override
    public IEdge<K, EV> getEdge(byte[] key, byte[] value) {
        IEdge edge = this.graphDataSchema.getEdgeConsFun().get();
        List<byte[]> values = bytesEncoder.split(key, StateConfigKeys.DELIMITER);
        if (values == null) {
            IBytesEncoder encoder = BytesEncoderRepo.get(
                bytesEncoder.parseMagicNumber(key[key.length - 1]));
            values = encoder.split(key, StateConfigKeys.DELIMITER);
        }
        for (int i = 0; i < edgeSchema.size(); i++) {
            edgeSchema.get(i).setBinaryValue(edge, values.get(i), graphDataSchema);
        }
        return edge;
    }

    @Override
    public IBytesEncoder getBytesEncoder() {
        return this.bytesEncoder;
    }
}
