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

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import java.util.Map;
import org.apache.geaflow.model.graph.meta.GraphFiledName;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.schema.GraphDataSchema;

public enum VertexAtom {
    /**
     * ID VERTEX ATOM.
     */
    ID {
        @Override
        public <K, VV> Object getValue(IVertex<K, VV> vertex, GraphDataSchema graphDataSchema) {
            return vertex.getId();
        }

        @Override
        public <K, VV> byte[] getBinaryValue(IVertex<K, VV> vertex,
                                             GraphDataSchema graphDataSchema) {
            return graphDataSchema.getKeyType().serialize(vertex.getId());
        }

        @Override
        public <K, VV> void setValue(IVertex<K, VV> vertex, Object value,
                                     GraphDataSchema graphDataSchema) {
            vertex.setId((K) value);
        }

        @Override
        public <K, VV> void setBinaryValue(IVertex<K, VV> vertex, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            vertex.setId((K) graphDataSchema.getKeyType().deserialize(value));
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.ID;
        }
    },
    /**
     * TIME VERTEX ATOM.
     */
    TIME {
        @Override
        public <K, VV> Object getValue(IVertex<K, VV> vertex, GraphDataSchema graphDataSchema) {
            return graphDataSchema.getVertexMeta().getGraphFieldSerializer()
                .getValue(vertex, GraphFiledName.TIME);
        }

        @Override
        public <K, VV> byte[] getBinaryValue(IVertex<K, VV> vertex,
                                             GraphDataSchema graphDataSchema) {
            return Longs.toByteArray((Long) getValue(vertex, graphDataSchema));
        }

        @Override
        public <K, VV> void setValue(IVertex<K, VV> vertex, Object value,
                                     GraphDataSchema graphDataSchema) {
            graphDataSchema.getVertexMeta().getGraphFieldSerializer()
                .setValue(vertex, GraphFiledName.TIME, value);
        }

        @Override
        public <K, VV> void setBinaryValue(IVertex<K, VV> vertex, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            setValue(vertex, Longs.fromByteArray(value), graphDataSchema);
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.TIME;
        }
    },
    DESC_TIME {
        @Override
        public <K, VV> Object getValue(IVertex<K, VV> vertex, GraphDataSchema graphDataSchema) {
            return Long.MAX_VALUE - (Long) TIME.getValue(vertex, graphDataSchema);
        }

        @Override
        public <K, VV> byte[] getBinaryValue(IVertex<K, VV> vertex,
                                             GraphDataSchema graphDataSchema) {
            return Longs.toByteArray((Long) getValue(vertex, graphDataSchema));
        }

        @Override
        public <K, VV> void setValue(IVertex<K, VV> vertex, Object value,
                                     GraphDataSchema graphDataSchema) {
            graphDataSchema.getEdgeMeta().getGraphFieldSerializer().setValue(vertex,
                GraphFiledName.TIME, Long.MAX_VALUE - (Long) value);
        }

        @Override
        public <K, VV> void setBinaryValue(IVertex<K, VV> vertex, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            setValue(vertex, Longs.fromByteArray(value), graphDataSchema);
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.TIME;
        }
    },
    /**
     * LABEL VERTEX ATOM.
     */
    LABEL {
        public <K, VV> Object getValue(IVertex<K, VV> vertex,
                                       GraphDataSchema graphDataSchema) {
            return graphDataSchema.getVertexMeta().getGraphFieldSerializer()
                .getValue(vertex, GraphFiledName.LABEL);
        }

        @Override
        public <K, VV> byte[] getBinaryValue(IVertex<K, VV> vertex,
                                             GraphDataSchema graphDataSchema) {
            return getValue(vertex, graphDataSchema).toString().getBytes();
        }

        @Override
        public <K, VV> void setValue(IVertex<K, VV> vertex, Object value,
                                     GraphDataSchema graphDataSchema) {
            graphDataSchema.getVertexMeta().getGraphFieldSerializer()
                .setValue(vertex, GraphFiledName.LABEL, value);
        }

        @Override
        public <K, VV> void setBinaryValue(IVertex<K, VV> vertex, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            setValue(vertex, new String(value), graphDataSchema);
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.TIME;
        }
    };

    public abstract <K, VV> Object getValue(IVertex<K, VV> vertex, GraphDataSchema graphDataSchema);

    public abstract <K, VV> byte[] getBinaryValue(IVertex<K, VV> vertex,
                                                  GraphDataSchema graphDataSchema);

    public abstract <K, VV> void setValue(IVertex<K, VV> vertex, Object value,
                                          GraphDataSchema graphDataSchema);

    public abstract <K, VV> void setBinaryValue(IVertex<K, VV> vertex, byte[] value,
                                                GraphDataSchema graphDataSchema);

    public abstract GraphFiledName getGraphFieldName();

    public static final Map<GraphFiledName, VertexAtom> VERTEX_ATOM_MAP = ImmutableMap.of(
        GraphFiledName.ID, ID,
        GraphFiledName.TIME, TIME,
        GraphFiledName.LABEL, LABEL
    );

    public static VertexAtom getEnum(String value) {
        for (VertexAtom v : values()) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }
        return null;
    }

}
