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

import com.google.common.base.Splitter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.schema.GraphDataSchema;

public class GraphKVEncoderFactory {

    private static final char EDGE_ORDER_SPLITTER = ',';

    public static <K, VV, EV> IGraphKVEncoder<K, VV, EV> build(Configuration config,
                                                               GraphDataSchema schema) {
        String clazz = config.getString(StateConfigKeys.STATE_KV_ENCODER_CLASS);
        String edgeOrder = config.getString(StateConfigKeys.STATE_KV_ENCODER_EDGE_ORDER);
        if (edgeOrder != null && edgeOrder.length() > 0) {
            List<EdgeAtom> list = Splitter.on(EDGE_ORDER_SPLITTER).splitToList(edgeOrder).stream()
                .map(c -> EdgeAtom.getEnum(c.trim())).collect(Collectors.toList());
            schema.setEdgeAtoms(list);
        }
        try {
            IGraphKVEncoder<K, VV, EV> encoder = (IGraphKVEncoder) Class.forName(clazz).newInstance();
            encoder.init(schema);
            return encoder;
        } catch (Exception e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError(e.getMessage()), e);
        }
    }
}
