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

import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.schema.GraphDataSchema;

public class EdgeKVEncoder<K, EV> extends EdgeKVEncoderWithoutValue<K, EV> {

    public EdgeKVEncoder(GraphDataSchema graphDataSchema, IBytesEncoder bytesEncoder) {
        super(graphDataSchema, bytesEncoder);
    }

    @Override
    public Tuple<byte[], byte[]> format(IEdge<K, EV> edge) {
        Tuple<byte[], byte[]> res = super.format(edge);
        res.f1 = graphDataSchema.getEdgePropertySerFun().apply(edge.getValue());
        return res;
    }

    @Override
    public IEdge<K, EV> getEdge(byte[] key, byte[] value) {
        IEdge edge = super.getEdge(key, value);
        return edge.withValue(graphDataSchema.getEdgePropertyDeFun().apply(value));
    }
}
