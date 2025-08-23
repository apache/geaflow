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

package org.apache.geaflow.dsl.common.data;

import static org.apache.geaflow.dsl.common.util.TestSchemaUtil.getEdge;
import static org.apache.geaflow.dsl.common.util.TestSchemaUtil.getIdTypes;
import static org.apache.geaflow.dsl.common.util.TestSchemaUtil.getSrcId;
import static org.apache.geaflow.dsl.common.util.TestSchemaUtil.getTargetId;
import static org.apache.geaflow.dsl.common.util.TestSchemaUtil.getVertex;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.utils.ClassUtil;
import org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedPath;
import org.apache.geaflow.dsl.common.data.impl.DefaultPath;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.ParameterizedPath;
import org.apache.geaflow.dsl.common.data.impl.types.BinaryStringEdge;
import org.apache.geaflow.dsl.common.data.impl.types.BinaryStringTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleEdge;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleVertex;
import org.apache.geaflow.dsl.common.data.impl.types.IntEdge;
import org.apache.geaflow.dsl.common.data.impl.types.IntTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.IntVertex;
import org.apache.geaflow.dsl.common.data.impl.types.LongEdge;
import org.apache.geaflow.dsl.common.data.impl.types.LongTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectTsEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectVertex;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.testng.annotations.Test;

public class BasicDataTest {

    @Test
    public void testDefaultPath() {
        Object[] fields = new Object[]{"test", 1L, 1.0};
        DefaultPath emptyPath = new DefaultPath();
        assertEquals(emptyPath.getPathNodes().size(), 0);
        emptyPath.addNode(ObjectRow.create(fields));
        assertEquals(emptyPath.size(), 1);
        assertEquals(emptyPath.subPath(Lists.newArrayList(0)).size(), 1);
    }

    @Test
    public void testObjectEdge() {
        Object[] fields = new Object[]{"test", 1L, 1.0};
        ObjectEdge emptyEdge =
            new ObjectEdge(1, 2);
        ObjectEdge rowEdge =
            new ObjectEdge(1, 2, ObjectRow.create(fields));
        assertNull(rowEdge.getBinaryLabel());
        assertEquals(rowEdge.getSrcId(), 1);
        rowEdge.setSrcId(2);
        assertEquals(rowEdge.getSrcId(), 2);
        assertEquals(rowEdge.getTargetId(), 2);
        rowEdge.setTargetId(1);
        assertEquals(rowEdge.getTargetId(), 1);
        assertEquals(rowEdge.getValue().toString(), "[test, 1, 1.0]");
        assertEquals(rowEdge.getDirect(), EdgeDirection.OUT);

        ObjectEdge reverse = rowEdge.reverse();
        assertEquals(reverse.getSrcId(), rowEdge.getTargetId());
        assertEquals(reverse.getTargetId(), rowEdge.getSrcId());

        ObjectEdge rowEdge2 = rowEdge.withValue(ObjectRow.create(fields));
        assertEquals(rowEdge2.getField(0, null), 2);
        assertEquals(rowEdge2.getField(1, null), 1);
        assertNull(rowEdge2.getField(2, null));
        assertEquals(rowEdge2.getField(3, null), "test");
        assertEquals(rowEdge2, rowEdge);
        assertNotEquals(emptyEdge, rowEdge);
    }

    @Test
    public void testEdge() {
        Object[] fields = new Object[]{"test", 1L, 1.0};
        IType<?>[] idTypes = getIdTypes();
        for (IType<?> idType : idTypes) {
            RowEdge rowEdge = getEdge(idType, ObjectRow.create(fields), false);
            assertEquals(rowEdge.getBinaryLabel(), BinaryString.fromString("edgeLabel"));
            assertEquals(rowEdge.getSrcId(), getSrcId(idType));
            assertEquals(rowEdge.getValue().toString(), "[test, 1, 1.0]");
            assertEquals(rowEdge.getDirect(), EdgeDirection.IN);
            RowEdge reverse = (RowEdge) rowEdge.reverse();
            assertEquals(reverse.getSrcId(), rowEdge.getTargetId());
            assertEquals(reverse.getTargetId(), rowEdge.getSrcId());
            RowEdge rowEdge2 = (RowEdge) rowEdge.withValue(ObjectRow.create(fields));
            rowEdge2.setLabel("relation");
            assertEquals(rowEdge2.getLabel(), "relation");
            assertEquals(rowEdge2.getField(0, null), getSrcId(idType));
            assertEquals(rowEdge2.getField(1, null), getTargetId(idType));
            assertEquals(rowEdge2.getField(2, null), BinaryString.fromString("relation"));
            assertEquals(rowEdge2.getField(3, null), "test");
            assertNotEquals(rowEdge2, rowEdge);
            assertEquals(reverse.reverse(), rowEdge);
        }
    }

    @Test
    public void testObjectTsEdge() {
        Object[] fields = new Object[]{"test", 1L, 1.0};
        ObjectTsEdge rowEdge =
            new ObjectTsEdge(1, 2, ObjectRow.create(fields));
        assertEquals(rowEdge.getTime(), 0);
        rowEdge.setTime(1);
        assertEquals(rowEdge.getTime(), 1);
        ObjectTsEdge reverse = rowEdge.reverse();
        ObjectTsEdge rowEdge2 = (ObjectTsEdge) rowEdge.withValue(ObjectRow.create(fields));
        assertEquals(rowEdge2.getField(0, null), 1);
        assertEquals(rowEdge2.getField(1, null), 2);
        assertNull(rowEdge2.getField(2, null));
        assertEquals(rowEdge2.getField(3, null), 1L);
        assertEquals(rowEdge2.getField(4, null), "test");
        assertEquals(rowEdge2, rowEdge);
        assertNotEquals(reverse, rowEdge);
    }

    @Test
    public void testEdgeWithTs() {
        Object[] fields = new Object[]{"test", 1L, 1.0};
        IType<?>[] idTypes = getIdTypes();
        for (IType<?> idType : idTypes) {
            RowEdge rowEdge = getEdge(idType, ObjectRow.create(fields), true);
            ((IGraphElementWithTimeField) rowEdge).setTime(1L);
            assertEquals(((IGraphElementWithTimeField) rowEdge).getTime(), 1L);
            assertEquals(rowEdge.getBinaryLabel(), BinaryString.fromString("edgeLabel"));
            assertEquals(rowEdge.getSrcId(), getSrcId(idType));
            assertEquals(rowEdge.getValue().toString(), "[test, 1, 1.0]");
            assertEquals(rowEdge.getDirect(), EdgeDirection.IN);
            RowEdge reverse = (RowEdge) rowEdge.reverse();
            assertEquals(reverse.getSrcId(), rowEdge.getTargetId());
            assertEquals(reverse.getTargetId(), rowEdge.getSrcId());
            RowEdge rowEdge2 = (RowEdge) rowEdge.withValue(ObjectRow.create(fields));
            rowEdge2.setLabel("relation");
            assertEquals(rowEdge2.getLabel(), "relation");
            assertEquals(rowEdge2.getField(0, null), getSrcId(idType));
            assertEquals(rowEdge2.getField(1, null), getTargetId(idType));
            assertEquals(rowEdge2.getField(2, null), BinaryString.fromString("relation"));
            assertEquals(rowEdge2.getField(3, null), 1L);
            assertEquals(rowEdge2.getField(4, null), "test");
            assertNotEquals(rowEdge2, rowEdge);
            assertEquals(reverse.reverse(), rowEdge);
        }
    }

    @Test
    public void testObjectVertex() {
        Object[] fields = new Object[]{"test", 1L, 1.0};
        ObjectVertex emptyVertex = new ObjectVertex(2);
        ObjectVertex rowVertex =
            new ObjectVertex(1, BinaryString.fromString("person"), ObjectRow.create(fields));
        assertEquals(rowVertex.getLabel(), "person");
        assertEquals(rowVertex.getId(), 1);
        assertEquals(rowVertex.getValue().toString(), "[test, 1, 1.0]");
        ObjectVertex rowVertex2 = rowVertex.withValue(ObjectRow.create(fields));
        rowVertex2 = rowVertex2.withLabel("user");
        assertEquals(rowVertex2.getLabel(), "user");
        assertEquals(rowVertex.compareTo(rowVertex2), 0);
        assertEquals(rowVertex2.getField(0, null), 1);
        assertEquals(rowVertex2.getField(1, null), BinaryString.fromString("user"));
        assertEquals(rowVertex2.getField(2, null), "test");
        assertNotEquals(rowVertex2, rowVertex);
        assertNotEquals(emptyVertex, rowVertex);
    }

    @Test
    public void testVertex() {
        Object[] fields = new Object[]{"test", 1L, 1.0};
        IType<?>[] idTypes = getIdTypes();
        for (IType<?> idType : idTypes) {
            RowVertex rowVertex = getVertex(idType, ObjectRow.create(fields));
            assertEquals(rowVertex.getBinaryLabel(), BinaryString.fromString("vertexLabel"));
            assertEquals(rowVertex.getId(), getSrcId(idType));
            assertEquals(rowVertex.getValue().toString(), "[test, 1, 1.0]");
            RowVertex rowVertex2 = (RowVertex) rowVertex.withValue(ObjectRow.create(fields));
            rowVertex2 = (RowVertex) rowVertex2.withLabel("user");
            assertEquals(rowVertex.compareTo(rowVertex2), 0);
            assertEquals(rowVertex2.getField(0, null), getSrcId(idType));
            assertEquals(rowVertex2.getField(1, null), BinaryString.fromString("user"));
            assertEquals(rowVertex2.getField(2, null), "test");
            assertNotEquals(rowVertex2, rowVertex);
        }
    }

    @Test
    public void testVertexEqual() {
        IntVertex intVertex = new IntVertex(123);
        assertEquals(intVertex.id, 123);
        ObjectVertex objectVertex = new ObjectVertex(new Integer(123));
        assertTrue(intVertex.equals(objectVertex));
        DoubleVertex doubleVertex = new DoubleVertex(1.23);
        assertTrue(doubleVertex.id > 1.22);
    }

    @Test
    public void testEdgeEqual() {
        LongEdge longEdge = new LongEdge(12L, 23L);
        ObjectEdge objectEdge = new ObjectEdge(new Long(12L), new Long(23L));
        assertFalse(longEdge.equals(objectEdge));
        IntEdge intEdge = new IntEdge(12, 23);
        ObjectEdge objectEdge2 = new ObjectEdge(new Integer(12), new Integer(23));
        assertFalse(intEdge.equals(objectEdge2));
    }

    @Test
    public void testEdgeIdentityReverse() {
        testEdgeIdentityReverse(BinaryStringEdge.class, BinaryString.fromString("1"), BinaryString.fromString("2"));
        testEdgeIdentityReverse(BinaryStringTsEdge.class, BinaryString.fromString("1"), BinaryString.fromString("2"));
        testEdgeIdentityReverse(DoubleEdge.class, 1.0, 2.0);
        testEdgeIdentityReverse(DoubleTsEdge.class, 1.0, 2.0);
        testEdgeIdentityReverse(IntEdge.class, 1, 2);
        testEdgeIdentityReverse(IntTsEdge.class, 1, 2);
        testEdgeIdentityReverse(LongEdge.class, 1L, 2L);
        testEdgeIdentityReverse(LongTsEdge.class, 1L, 2L);
        testEdgeIdentityReverse(ObjectEdge.class, 1, 2);
    }

    private void testEdgeIdentityReverse(Class<? extends RowEdge> edgeClass, Object srcId, Object targetId) {
        RowEdge edge = ClassUtil.newInstance(edgeClass);
        edge.setSrcId(srcId);
        edge.setTargetId(targetId);
        edge.setDirect(EdgeDirection.OUT);
        RowEdge identityReverse = edge.identityReverse();
        assertEquals(identityReverse.getClass(), edgeClass);
        assertEquals(identityReverse.getSrcId(), edge.getTargetId());
        assertEquals(identityReverse.getTargetId(), edge.getSrcId());
        assertEquals(identityReverse.getDirect(), EdgeDirection.IN);
    }

    @Test
    public void testParameterizedPath() {
        Path basePath = new DefaultPath();
        basePath.addNode(new ObjectVertex());
        basePath.addNode(new ObjectEdge());

        DefaultParameterizedPath path = new DefaultParameterizedPath(basePath,
            1L, null, null);
        assertEquals(path.getPathNodes(), basePath.getPathNodes());

        path.addNode(new ObjectVertex());
        assertEquals(path.getPathNodes().size(), 3);

        path.remove(2);
        assertEquals(path.getPathNodes().size(), 2);

        ParameterizedPath subPath = (ParameterizedPath) path.subPath(new int[]{0});
        assertEquals(subPath.getSystemVariables(), path.getSystemVariables());
        assertEquals(subPath.getPathNodes().size(), 1);
    }
}
