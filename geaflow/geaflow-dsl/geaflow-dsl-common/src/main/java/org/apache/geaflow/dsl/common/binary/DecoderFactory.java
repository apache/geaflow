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
import org.apache.geaflow.dsl.common.binary.decoder.DefaultEdgeDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.DefaultPathDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.DefaultRowDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.DefaultVertexDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.EdgeDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.IBinaryDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.PathDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.RowDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.VertexDecoder;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.VertexType;

public class DecoderFactory {

    public static IBinaryDecoder createDecoder(IType<?> type) {
        String typeName = type.getName().toUpperCase(Locale.ROOT);
        switch (typeName) {
            case Types.TYPE_NAME_VERTEX:
                return new DefaultVertexDecoder((VertexType) type);
            case Types.TYPE_NAME_EDGE:
                return new DefaultEdgeDecoder((EdgeType) type);
            case Types.TYPE_NAME_STRUCT:
                return new DefaultRowDecoder((StructType) type);
            case Types.TYPE_NAME_PATH:
                return new DefaultPathDecoder((PathType) type);
            default:
                throw new GeaFlowDSLException("decoder type " + type.getName() + " is not support");
        }
    }

    public static VertexDecoder createVertexDecoder(VertexType vertexType) {
        return new DefaultVertexDecoder(vertexType);
    }

    public static EdgeDecoder createEdgeDecoder(EdgeType edgeType) {
        return new DefaultEdgeDecoder(edgeType);
    }

    public static RowDecoder createRowDecoder(StructType rowType) {
        return new DefaultRowDecoder(rowType);
    }

    public static PathDecoder createPathDecoder(PathType pathType) {
        return new DefaultPathDecoder(pathType);
    }
}
