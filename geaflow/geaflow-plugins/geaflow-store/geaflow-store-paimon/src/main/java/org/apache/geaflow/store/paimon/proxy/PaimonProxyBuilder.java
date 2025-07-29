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

package org.apache.geaflow.store.paimon.proxy;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.store.paimon.PaimonTableRWHandle;

public class PaimonProxyBuilder {

    public static <K, VV, EV> IGraphPaimonProxy<K, VV, EV> build(Configuration config,
                                                                 PaimonTableRWHandle vertexRWHandle,
                                                                 PaimonTableRWHandle edgeRWHandle,
                                                                 int[] projection,
                                                                 IGraphKVEncoder<K, VV, EV> encoder) {
        // TODO: add readonly proxy.
        return new PaimonGraphRWProxy<>(vertexRWHandle, edgeRWHandle, projection, encoder);
    }

    public static <K, VV, EV> IGraphMultiVersionedPaimonProxy<K, VV, EV> buildMultiVersioned(
        Configuration config, PaimonTableRWHandle vertexRWHandle,
        PaimonTableRWHandle vertexIndexRWHandle, PaimonTableRWHandle edgeRWHandle, int[] projection,
        IGraphKVEncoder<K, VV, EV> encoder) {
        return new PaimonGraphMultiVersionedProxy<>(vertexRWHandle, vertexIndexRWHandle,
            edgeRWHandle, projection, encoder, config);
    }

}
