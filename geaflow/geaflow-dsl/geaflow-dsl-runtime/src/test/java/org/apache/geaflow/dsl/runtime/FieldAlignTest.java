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

package org.apache.geaflow.dsl.runtime;

import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.types.IntEdge;
import org.apache.geaflow.dsl.common.data.impl.types.IntVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignEdge;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignVertex;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldAlignTest {

    @Test
    public void testFieldAlignEdge() {
        RowEdge edge = new IntEdge(1, 2);
        edge.setValue(ObjectRow.create(1, 2, 3));
        edge.setDirect(EdgeDirection.OUT);

        FieldAlignEdge alignEdge = new FieldAlignEdge(edge, new int[]{0, 1, 2, 3, -1, 4, 5});

        Assert.assertEquals(alignEdge.getSrcId(), edge.getSrcId());
        Assert.assertEquals(alignEdge.getTargetId(), edge.getTargetId());
        Assert.assertEquals(alignEdge.getDirect(), edge.getDirect());

        Assert.assertEquals(alignEdge.getField(3, Types.INTEGER), 1);
        Assert.assertNull(alignEdge.getField(4, Types.INTEGER));
        Assert.assertEquals(alignEdge.getField(6, Types.INTEGER), 3);

        alignEdge.setLabel("l0");
        Assert.assertEquals(alignEdge.getLabel(), "l0");
        alignEdge.setBinaryLabel(BinaryString.fromString("l1"));
        Assert.assertEquals(alignEdge.getBinaryLabel(), BinaryString.fromString("l1"));

        alignEdge = (FieldAlignEdge) alignEdge.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(alignEdge.getValue(), ObjectRow.EMPTY);

        alignEdge = (FieldAlignEdge) alignEdge.withDirection(EdgeDirection.IN);
        Assert.assertEquals(alignEdge.getDirect(), EdgeDirection.IN);

        alignEdge.setSrcId(2);
        alignEdge.setTargetId(1);
        alignEdge.setDirect(EdgeDirection.OUT);
        Assert.assertEquals(alignEdge.getSrcId(), 2);
        Assert.assertEquals(alignEdge.getTargetId(), 1);
        Assert.assertEquals(alignEdge.getDirect(), EdgeDirection.OUT);

        alignEdge = (FieldAlignEdge) alignEdge.identityReverse();
        Assert.assertEquals(alignEdge.getDirect(), EdgeDirection.IN);
        Assert.assertEquals(alignEdge.getSrcId(), 1);
        Assert.assertEquals(alignEdge.getTargetId(), 2);
    }

    @Test
    public void testAlignVertex() {
        RowVertex vertex = new IntVertex(1);
        vertex.setValue(ObjectRow.create(1, 2, 3));

        FieldAlignVertex alignVertex = new FieldAlignVertex(vertex, new int[]{-1, 2, 1, 0});
        Assert.assertEquals(alignVertex.getId(), vertex.getId());
        Assert.assertNull(alignVertex.getField(0, Types.INTEGER));
        Assert.assertEquals(alignVertex.getField(1, Types.INTEGER), 1);

        alignVertex.setId(2);
        Assert.assertEquals(alignVertex.getId(), 2);
        alignVertex.setLabel("l0");
        Assert.assertEquals(alignVertex.getLabel(), "l0");
        alignVertex.setBinaryLabel(BinaryString.fromString("l1"));
        Assert.assertEquals(alignVertex.getBinaryLabel(), BinaryString.fromString("l1"));

        alignVertex.setValue(ObjectRow.EMPTY);
        Assert.assertEquals(alignVertex.getValue(), ObjectRow.EMPTY);
        alignVertex = (FieldAlignVertex) alignVertex.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(alignVertex.getValue(), ObjectRow.EMPTY);

        alignVertex = (FieldAlignVertex) alignVertex.withLabel("l2");
        Assert.assertEquals(alignVertex.getBinaryLabel(), BinaryString.fromString("l2"));
    }
}
