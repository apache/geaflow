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

package org.apache.geaflow.dsl.runtime.data;

import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleEdge;
import org.apache.geaflow.dsl.common.data.impl.types.IntEdge;
import org.apache.geaflow.dsl.common.data.impl.types.LongEdge;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignDoubleEdge;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignIntEdge;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignLongEdge;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RuntimeEdgeTest {

    @Test
    public void testLongEdge() {
        LongEdge test = new LongEdge(1, 2);
        test.withDirection(EdgeDirection.OUT);
        test.withValue(ObjectRow.EMPTY);
        LongEdge test2 = new LongEdge(2, 1);
        test.withDirection(EdgeDirection.OUT);
        test2.withValue(ObjectRow.EMPTY);
        test2 = test2.reverse();
        Assert.assertEquals(test, test2);

        FieldAlignLongEdge test3 = new FieldAlignLongEdge(test2, new int[]{0, 1});
        test3.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test, test3.reverse().reverse());
        FieldAlignLongEdge test4 = new FieldAlignLongEdge(test, new int[]{0, 1});
        Assert.assertEquals(test3, test4);
    }

    @Test
    public void testIntEdge() {
        IntEdge test = new IntEdge(1, 2);
        test.withDirection(EdgeDirection.OUT);
        test.withValue(ObjectRow.EMPTY);
        IntEdge test2 = new IntEdge(2, 1);
        test.withDirection(EdgeDirection.OUT);
        test2.withValue(ObjectRow.EMPTY);
        test2 = test2.reverse();
        Assert.assertEquals(test, test2);

        FieldAlignIntEdge test3 = new FieldAlignIntEdge(test2, new int[]{0, 1});
        test3.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test, test3.reverse().reverse());
        FieldAlignIntEdge test4 = new FieldAlignIntEdge(test, new int[]{0, 1});
        Assert.assertEquals(test3, test4);
    }

    @Test
    public void testDoubleEdge() {
        DoubleEdge test = new DoubleEdge(1, 2);
        test.withDirection(EdgeDirection.OUT);
        test.withValue(ObjectRow.EMPTY);
        DoubleEdge test2 = new DoubleEdge(2, 1);
        test.withDirection(EdgeDirection.OUT);
        test2.withValue(ObjectRow.EMPTY);
        test2 = test2.reverse();
        Assert.assertEquals(test, test2);

        FieldAlignDoubleEdge test3 = new FieldAlignDoubleEdge(test2, new int[]{0, 1});
        test3.withValue(ObjectRow.EMPTY);
        Assert.assertEquals(test, test3.reverse().reverse());
        FieldAlignDoubleEdge test4 = new FieldAlignDoubleEdge(test, new int[]{0, 1});
        Assert.assertEquals(test3, test4);
    }
}

