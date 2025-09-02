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

package org.apache.geaflow.dsl.rel.match;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit test class for MatchSamePredicate.
 * Tests the creation, copying, and basic functionality of MatchSamePredicate nodes.
 */
public class MatchSamePredicateTest {

    @Mock
    private RelOptCluster cluster;

    @Mock
    private RelTraitSet traitSet;

    @Mock
    private IMatchNode left;

    @Mock
    private IMatchNode right;

    @Mock
    private RexNode condition;

    @Mock
    private PathRecordType pathSchema;

    private RelDataTypeFactory typeFactory;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        
        typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        
        // Setup cluster mock
        when(cluster.getTypeFactory()).thenReturn(typeFactory);
        
        // Setup trait set mock
        // Note: RelTraitSet doesn't have getConvention method, so we skip this
        
        // Setup left and right match nodes
        when(left.getCluster()).thenReturn(cluster);
        when(left.getTraitSet()).thenReturn(traitSet);
        when(left.getNodeType()).thenReturn(pathSchema);
        
        when(right.getCluster()).thenReturn(cluster);
        when(right.getTraitSet()).thenReturn(traitSet);
        when(right.getNodeType()).thenReturn(pathSchema);
        
        // Setup path schema mock
        when(pathSchema.getFieldCount()).thenReturn(3);
        when(pathSchema.getFieldNames()).thenReturn(Arrays.asList("a", "b", "c"));
    }

    /**
     * Test creating a MatchSamePredicate with distinct semantics
     */
    @Test
    public void testCreateWithDistinct() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        assertNotNull("MatchSamePredicate should be created", samePredicate);
        assertTrue("Should be distinct", samePredicate.isDistinct());
        assertFalse("Should not be union all", samePredicate.isUnionAll());
        assertEquals("Left should match", left, samePredicate.getLeft());
        assertEquals("Right should match", right, samePredicate.getRight());
        assertEquals("Condition should match", condition, samePredicate.getCondition());
    }

    /**
     * Test creating a MatchSamePredicate with union all semantics
     */
    @Test
    public void testCreateWithUnionAll() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, false, pathSchema);
        
        assertNotNull("MatchSamePredicate should be created", samePredicate);
        assertFalse("Should not be distinct", samePredicate.isDistinct());
        assertTrue("Should be union all", samePredicate.isUnionAll());
    }

    /**
     * Test getting inputs from MatchSamePredicate
     */
    @Test
    public void testGetInputs() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        List<RelNode> inputs = samePredicate.getInputs();
        assertEquals("Should have 2 inputs", 2, inputs.size());
        assertEquals("First input should be left", left, inputs.get(0));
        assertEquals("Second input should be right", right, inputs.get(1));
    }

    /**
     * Test getting node type from MatchSamePredicate
     */
    @Test
    public void testGetNodeType() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        RelDataType nodeType = samePredicate.getNodeType();
        assertEquals("Node type should match left node type", pathSchema, nodeType);
    }

    /**
     * Test getting path schema from MatchSamePredicate
     */
    @Test
    public void testGetPathSchema() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        PathRecordType schema = samePredicate.getPathSchema();
        assertEquals("Path schema should match", pathSchema, schema);
    }

    /**
     * Test copying MatchSamePredicate with new inputs
     */
    @Test
    public void testCopyWithNewInputs() {
        MatchSamePredicate original = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        IMatchNode newLeft = mock(IMatchNode.class);
        IMatchNode newRight = mock(IMatchNode.class);
        when(newLeft.getCluster()).thenReturn(cluster);
        when(newLeft.getTraitSet()).thenReturn(traitSet);
        when(newRight.getCluster()).thenReturn(cluster);
        when(newRight.getTraitSet()).thenReturn(traitSet);
        
        List<RelNode> newInputs = Arrays.asList(newLeft, newRight);
        IMatchNode copied = original.copy(newInputs, pathSchema);
        
        assertNotNull("Copied node should not be null", copied);
        assertTrue("Copied node should be MatchSamePredicate", copied instanceof MatchSamePredicate);
        
        MatchSamePredicate copiedSamePredicate = (MatchSamePredicate) copied;
        assertEquals("New left should match", newLeft, copiedSamePredicate.getLeft());
        assertEquals("New right should match", newRight, copiedSamePredicate.getRight());
        assertEquals("Condition should be preserved", condition, copiedSamePredicate.getCondition());
        assertEquals("Distinct flag should be preserved", original.isDistinct(), copiedSamePredicate.isDistinct());
    }

    /**
     * Test copying MatchSamePredicate with new trait set
     */
    @Test
    public void testCopyWithNewTraitSet() {
        MatchSamePredicate original = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        RelTraitSet newTraitSet = mock(RelTraitSet.class);
        // Note: RelTraitSet doesn't have getConvention method, so we skip this
        
        List<RelNode> inputs = Arrays.asList(left, right);
        RelNode copied = original.copy(newTraitSet, inputs);
        
        assertNotNull("Copied node should not be null", copied);
        assertTrue("Copied node should be MatchSamePredicate", copied instanceof MatchSamePredicate);
        
        MatchSamePredicate copiedSamePredicate = (MatchSamePredicate) copied;
        assertEquals("Trait set should be updated", newTraitSet, copiedSamePredicate.getTraitSet());
        assertEquals("Left should be preserved", left, copiedSamePredicate.getLeft());
        assertEquals("Right should be preserved", right, copiedSamePredicate.getRight());
        assertEquals("Condition should be preserved", condition, copiedSamePredicate.getCondition());
    }

    /**
     * Test accept method throws UnsupportedOperationException
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testAcceptThrowsException() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        // This should throw UnsupportedOperationException until visitSamePredicate is implemented
        samePredicate.accept(mock(org.apache.geaflow.dsl.rel.MatchNodeVisitor.class));
    }

    /**
     * Test toString method
     */
    @Test
    public void testToString() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        String result = samePredicate.toString();
        assertNotNull("toString should not return null", result);
        assertTrue("toString should contain class name", result.contains("MatchSamePredicate"));
        assertTrue("toString should contain left", result.contains("left="));
        assertTrue("toString should contain right", result.contains("right="));
        assertTrue("toString should contain condition", result.contains("condition="));
        assertTrue("toString should contain distinct", result.contains("distinct="));
    }

    /**
     * Test that MatchSamePredicate implements IMatchNode interface
     */
    @Test
    public void testImplementsIMatchNode() {
        MatchSamePredicate samePredicate = MatchSamePredicate.create(
            left, right, condition, true, pathSchema);
        
        assertTrue("Should implement IMatchNode", samePredicate instanceof IMatchNode);
    }
}
