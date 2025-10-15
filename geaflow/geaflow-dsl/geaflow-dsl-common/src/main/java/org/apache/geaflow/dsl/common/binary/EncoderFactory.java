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

package org.apache.geaflow.dsl.common.binary;

import java.util.Locale;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.binary.encoder.DefaultEdgeEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.DefaultRowEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.DefaultVertexEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.EdgeEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.IBinaryEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.RowEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.VertexEncoder;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.VertexType;

public class EncoderFactory {

    public static IBinaryEncoder createEncoder(IType<?> type) {
        String typeName = type.getName().toUpperCase(Locale.ROOT);
        switch (typeName) {
            case Types.TYPE_NAME_VERTEX:
                return new DefaultVertexEncoder((VertexType) type);
            case Types.TYPE_NAME_EDGE:
                return new DefaultEdgeEncoder((EdgeType) type);
            case Types.TYPE_NAME_STRUCT:
                return new DefaultRowEncoder((StructType) type);
            default:
                throw new GeaFlowDSLException("encoder type " + type.getName() + " is not support");
        }
    }

    public static VertexEncoder createVertexEncoder(VertexType vertexType) {
        return new DefaultVertexEncoder(vertexType);
    }

    public static EdgeEncoder createEdgeEncoder(EdgeType edgeType) {
        return new DefaultEdgeEncoder(edgeType);
    }

    public static RowEncoder createRowEncoder(StructType rowType) {
        return new DefaultRowEncoder(rowType);
    }
}
