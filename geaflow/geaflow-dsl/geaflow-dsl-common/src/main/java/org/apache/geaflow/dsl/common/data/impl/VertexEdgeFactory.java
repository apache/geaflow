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

package org.apache.geaflow.dsl.common.data.impl;

import java.util.Locale;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.types.BinaryStringEdge;
import org.apache.geaflow.dsl.common.data.impl.types.BinaryStringTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.BinaryStringVertex;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleEdge;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleVertex;
import org.apache.geaflow.dsl.common.data.impl.types.IntEdge;
import org.apache.geaflow.dsl.common.data.impl.types.IntTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.IntVertex;
import org.apache.geaflow.dsl.common.data.impl.types.LongEdge;
import org.apache.geaflow.dsl.common.data.impl.types.LongTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.LongVertex;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectVertex;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.VertexType;

public class VertexEdgeFactory {

    public static RowVertex createVertex(VertexType vertexType) {
        String idTypeName = vertexType.getId().getType().getName().toUpperCase(Locale.ROOT);
        switch (idTypeName) {
            case Types.TYPE_NAME_INTEGER:
                return new IntVertex();
            case Types.TYPE_NAME_LONG:
                return new LongVertex();
            case Types.TYPE_NAME_DOUBLE:
                return new DoubleVertex();
            case Types.TYPE_NAME_BINARY_STRING:
                return new BinaryStringVertex();
            default:
        }
        return new ObjectVertex();
    }

    public static RowEdge createEdge(EdgeType edgeType) {
        String idTypeName = edgeType.getSrcId().getType().getName().toUpperCase(Locale.ROOT);
        if (edgeType.getTimestamp().isPresent()) {
            return createTsEdge(idTypeName);
        }
        switch (idTypeName) {
            case Types.TYPE_NAME_INTEGER:
                return new IntEdge();
            case Types.TYPE_NAME_LONG:
                return new LongEdge();
            case Types.TYPE_NAME_DOUBLE:
                return new DoubleEdge();
            case Types.TYPE_NAME_BINARY_STRING:
                return new BinaryStringEdge();
            default:
        }
        return new ObjectEdge();
    }

    private static RowEdge createTsEdge(String idTypeName) {
        switch (idTypeName) {
            case Types.TYPE_NAME_INTEGER:
                return new IntTsEdge();
            case Types.TYPE_NAME_LONG:
                return new LongTsEdge();
            case Types.TYPE_NAME_DOUBLE:
                return new DoubleTsEdge();
            case Types.TYPE_NAME_BINARY_STRING:
                return new BinaryStringTsEdge();
            default:
        }
        return new ObjectTsEdge();
    }
}
