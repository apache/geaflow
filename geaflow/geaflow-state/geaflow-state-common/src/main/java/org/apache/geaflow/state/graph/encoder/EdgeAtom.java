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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.comparators.ComparatorChain;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.meta.GraphFiledName;
import org.apache.geaflow.state.pushdown.inner.PushDownPb;
import org.apache.geaflow.state.schema.GraphDataSchema;

public enum EdgeAtom {
    /**
     * SRC EDGE ATOM.
     */
    SRC_ID {
        @Override
        public <K, EV> Object getValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return edge.getSrcId();
        }

        @Override
        public <K, EV> byte[] getBinaryValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return graphDataSchema.getKeyType().serialize(edge.getSrcId());
        }

        @Override
        public <K, EV> void setValue(IEdge<K, EV> edge, Object value,
                                     GraphDataSchema graphDataSchema) {
            edge.setSrcId((K) value);
        }

        @Override
        public <K, EV> void setBinaryValue(IEdge<K, EV> edge, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            edge.setSrcId((K) graphDataSchema.getKeyType().deserialize(value));
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.SRC_ID;
        }
    },
    /**
     * TARGET EDGE ATOM.
     */
    DST_ID {
        @Override
        public <K, EV> Object getValue(IEdge<K, EV> edge,
                                       GraphDataSchema graphDataSchema) {
            return edge.getTargetId();
        }

        @Override
        public <K, EV> byte[] getBinaryValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return graphDataSchema.getKeyType().serialize(edge.getTargetId());
        }

        @Override
        public <K, EV> void setValue(IEdge<K, EV> edge, Object value,
                                     GraphDataSchema graphDataSchema) {
            edge.setTargetId((K) value);
        }

        @Override
        public <K, EV> void setBinaryValue(IEdge<K, EV> edge, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            edge.setTargetId((K) graphDataSchema.getKeyType().deserialize(value));
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.DST_ID;
        }
    },
    /**
     * TIME EDGE ATOM.
     */
    TIME {
        @Override
        public <K, EV> Object getValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return graphDataSchema.getEdgeMeta().getGraphFieldSerializer().getValue(edge, GraphFiledName.TIME);
        }

        @Override
        public <K, EV> byte[] getBinaryValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return Longs.toByteArray((Long) getValue(edge, graphDataSchema));
        }

        @Override
        public <K, EV> void setValue(IEdge<K, EV> edge, Object value,
                                     GraphDataSchema graphDataSchema) {
            graphDataSchema.getEdgeMeta().getGraphFieldSerializer().setValue(edge, GraphFiledName.TIME, value);
        }

        @Override
        public <K, EV> void setBinaryValue(IEdge<K, EV> edge, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            setValue(edge, Longs.fromByteArray(value), graphDataSchema);
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.TIME;
        }
    },
    DESC_TIME {
        @Override
        public <K, EV> Object getValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return Long.MAX_VALUE - (Long) TIME.getValue(edge, graphDataSchema);
        }

        @Override
        public <K, EV> byte[] getBinaryValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return Longs.toByteArray((Long) getValue(edge, graphDataSchema));
        }

        @Override
        public <K, EV> void setValue(IEdge<K, EV> edge, Object value,
                                     GraphDataSchema graphDataSchema) {
            graphDataSchema.getEdgeMeta().getGraphFieldSerializer().setValue(edge,
                GraphFiledName.TIME, Long.MAX_VALUE - (Long) value);
        }

        @Override
        public <K, EV> void setBinaryValue(IEdge<K, EV> edge, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            setValue(edge, Longs.fromByteArray(value), graphDataSchema);
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.TIME;
        }
    },
    /**
     * DIRECTION EDGE ATOM.
     */
    DIRECTION {
        @Override
        public <K, EV> Object getValue(IEdge<K, EV> edge,
                                       GraphDataSchema graphDataSchema) {
            return edge.getDirect();
        }

        @Override
        public <K, EV> byte[] getBinaryValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return edge.getDirect() == EdgeDirection.IN ? new byte[]{0} : new byte[]{1};
        }

        @Override
        public <K, EV> void setValue(IEdge<K, EV> edge, Object value,
                                     GraphDataSchema graphDataSchema) {
            edge.setDirect((EdgeDirection) value);
        }

        @Override
        public <K, EV> void setBinaryValue(IEdge<K, EV> edge, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            if (value[0] == 0) {
                edge.setDirect(EdgeDirection.IN);
            } else {
                edge.setDirect(EdgeDirection.OUT);
            }
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.DIRECTION;
        }
    },
    /**
     * LABEL EDGE ATOM.
     */
    LABEL {
        @Override
        public <K, EV> Object getValue(IEdge<K, EV> edge,
                                       GraphDataSchema graphDataSchema) {
            return graphDataSchema.getEdgeMeta().getGraphFieldSerializer().getValue(edge, GraphFiledName.LABEL);
        }

        @Override
        public <K, EV> byte[] getBinaryValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema) {
            return getValue(edge, graphDataSchema).toString().getBytes();
        }

        @Override
        public <K, EV> void setValue(IEdge<K, EV> edge, Object value,
                                     GraphDataSchema graphDataSchema) {
            graphDataSchema.getEdgeMeta().getGraphFieldSerializer().setValue(edge, GraphFiledName.LABEL, value);
        }

        @Override
        public <K, EV> void setBinaryValue(IEdge<K, EV> edge, byte[] value,
                                           GraphDataSchema graphDataSchema) {
            setValue(edge, new String(value), graphDataSchema);
        }

        @Override
        public GraphFiledName getGraphFieldName() {
            return GraphFiledName.LABEL;
        }
    };

    private PushDownPb.SortType sortType;

    EdgeAtom() {
        sortType = PushDownPb.SortType.valueOf(this.name());
    }

    public abstract <K, EV> Object getValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema);

    public abstract <K, EV> byte[] getBinaryValue(IEdge<K, EV> edge, GraphDataSchema graphDataSchema);

    public abstract <K, EV> void setValue(IEdge<K, EV> edge, Object value, GraphDataSchema graphDataSchema);

    public abstract <K, EV> void setBinaryValue(IEdge<K, EV> edge, byte[] value, GraphDataSchema graphDataSchema);

    public abstract GraphFiledName getGraphFieldName();

    public PushDownPb.SortType toPbSortType() {
        return sortType;
    }

    public static final Map<GraphFiledName, EdgeAtom> EDGE_ATOM_MAP = ImmutableMap.of(
        GraphFiledName.SRC_ID, SRC_ID,
        GraphFiledName.DST_ID, DST_ID,
        GraphFiledName.TIME, TIME,
        GraphFiledName.DIRECTION, DIRECTION,
        GraphFiledName.LABEL, LABEL
    );


    public static EdgeAtom getEnum(String value) {
        for (EdgeAtom v : values()) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }
        return null;
    }

    public Comparator<IEdge> getComparator() {
        switch (this) {
            case TIME:
                return Comparator.comparingLong(o -> ((IGraphElementWithTimeField) o).getTime());
            case DESC_TIME:
                return Collections.reverseOrder(Comparator.comparingLong(o -> ((IGraphElementWithTimeField) o).getTime()));
            case DIRECTION:
                return (o1, o2) -> Integer.compare(o2.getDirect().ordinal(), o1.getDirect().ordinal());
            case DST_ID:
                return (o1, o2) -> ((Comparable) o1.getTargetId()).compareTo(o2.getTargetId());
            case SRC_ID:
                return (o1, o2) -> ((Comparable) o1.getSrcId()).compareTo(o2.getTargetId());
            case LABEL:
                return Comparator.comparing(o -> ((IGraphElementWithLabelField) o).getLabel());
            default:
                throw new RuntimeException("no comparator");
        }
    }

    public static Comparator<IEdge> getComparator(List<EdgeAtom> fields) {
        if (fields == null || fields.size() == 0) {
            return null;
        }
        ComparatorChain chain = new ComparatorChain();
        fields.forEach(f -> chain.addComparator(f.getComparator()));
        return chain;
    }
}
