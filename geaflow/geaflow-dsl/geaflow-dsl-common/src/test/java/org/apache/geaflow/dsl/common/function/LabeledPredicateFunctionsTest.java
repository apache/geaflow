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

package org.apache.geaflow.dsl.common.function;

import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LabeledPredicateFunctionsTest {

    private RowVertex vertex(String label) {
        ObjectVertex vertex = new ObjectVertex(1L);
        vertex.setLabel(label);
        return vertex;
    }

    private RowEdge edge(String label) {
        ObjectEdge edge = new ObjectEdge(1L, 2L);
        edge.setLabel(label);
        return edge;
    }

    @Test
    public void testIsLabeledOnVertex() {
        Assert.assertTrue(LabeledPredicateFunctions.isLabeled(vertex("person"), "person"));
        Assert.assertFalse(LabeledPredicateFunctions.isLabeled(vertex("person"), "software"));
    }

    @Test
    public void testIsLabeledOnEdge() {
        Assert.assertTrue(LabeledPredicateFunctions.isLabeled(edge("knows"), "knows"));
        Assert.assertFalse(LabeledPredicateFunctions.isLabeled(edge("knows"), "created"));
    }

    @Test
    public void testIsNotLabeled() {
        Assert.assertFalse(LabeledPredicateFunctions.isNotLabeled(vertex("person"), "person"));
        Assert.assertTrue(LabeledPredicateFunctions.isNotLabeled(vertex("person"), "software"));
    }

    @Test
    public void testThreeValuedLogicWithNullOperand() {
        // Null element or null label yields Unknown (null) for both predicates.
        Assert.assertNull(LabeledPredicateFunctions.isLabeled(null, "person"));
        Assert.assertNull(LabeledPredicateFunctions.isLabeled(vertex("person"), null));
        Assert.assertNull(LabeledPredicateFunctions.isNotLabeled(null, "person"));
        Assert.assertNull(LabeledPredicateFunctions.isNotLabeled(vertex("person"), null));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNonGraphElementIsRejected() {
        LabeledPredicateFunctions.isLabeled("not a graph element", "person");
    }
}
