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

package org.apache.geaflow.state.schema;

import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelEdge;
import org.apache.geaflow.model.graph.meta.GraphElementMetas;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.property.IPropertySerializable;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueTimeVertex;
import org.apache.geaflow.state.graph.encoder.EdgeAtom;
import org.apache.geaflow.state.graph.encoder.VertexAtom;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphDataSchemaTest {

    @Test
    public void test() throws Exception {
        GraphMeta graphMeta = new GraphMeta(new GraphMetaType(Types.STRING,
            ValueTimeVertex.class, ValueTimeVertex::new, String.class,
            ValueLabelEdge.class, ValueLabelEdge::new, ClassProperty.class));

        GraphDataSchema graphDataSchema = new GraphDataSchema(graphMeta);
        Assert.assertEquals(graphDataSchema.getKeyType(), Types.STRING);
        Assert.assertFalse(graphDataSchema.isEmptyEdgeProperty());
        Assert.assertFalse(graphDataSchema.isEmptyVertexProperty());

        IVertex vertex = graphDataSchema.getVertexConsFun().get();
        Assert.assertEquals(vertex.getClass(), graphMeta.getVertexMeta().getGraphElementClass());
        IEdge edge = graphDataSchema.getEdgeConsFun().get();
        Assert.assertEquals(edge.getClass(), graphMeta.getEdgeMeta().getGraphElementClass());

        Assert.assertTrue(graphDataSchema.getVertexAtoms().contains(VertexAtom.TIME));
        Assert.assertTrue(graphDataSchema.getEdgeAtoms().contains(EdgeAtom.LABEL));

        byte[] bytes = StringType.INSTANCE.serialize("foobar");
        Assert.assertEquals(graphDataSchema.getVertexPropertySerFun().apply("foobar"), bytes);
        Assert.assertEquals(graphDataSchema.getVertexPropertyDeFun().apply(bytes), "foobar");

        Assert.assertEquals(graphDataSchema.getEdgePropertySerFun().apply(new ClassProperty()), new byte[0]);
        Assert.assertEquals(graphDataSchema.getEdgePropertyDeFun().apply(new byte[0]), property);

        GraphElementMetas.clearCache();
        graphMeta = new GraphMeta(new GraphMetaType(Types.STRING,
            ValueTimeVertex.class, ValueTimeVertex::new, ClassProperty.class,
            ValueLabelEdge.class, ValueLabelEdge::new, String.class));
        graphDataSchema = new GraphDataSchema(graphMeta);

        bytes = StringType.INSTANCE.serialize("foobar");
        Assert.assertEquals(graphDataSchema.getEdgePropertySerFun().apply("foobar"), bytes);
        Assert.assertEquals(graphDataSchema.getEdgePropertyDeFun().apply(bytes), "foobar");

        Assert.assertEquals(graphDataSchema.getVertexPropertySerFun().apply(new ClassProperty()), new byte[0]);
        Assert.assertEquals(graphDataSchema.getVertexPropertyDeFun().apply(new byte[0]), property);
    }

    private static final ClassProperty property = new ClassProperty();

    public static final class ClassProperty implements IPropertySerializable {

        @Override
        public IPropertySerializable fromBinary(byte[] bytes) {
            return property;
        }

        @Override
        public byte[] toBytes() {
            return new byte[0];
        }

        @Override
        public IPropertySerializable clone() {
            return property;
        }
    }
}
