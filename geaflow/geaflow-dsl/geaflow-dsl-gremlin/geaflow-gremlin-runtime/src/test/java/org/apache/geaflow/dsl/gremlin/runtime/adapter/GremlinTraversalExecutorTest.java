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

package org.apache.geaflow.dsl.gremlin.runtime.adapter;

import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for GremlinTraversalExecutor implementations.
 */
public class GremlinTraversalExecutorTest {

    private GeaFlowGremlinTraversalExecutor executor;

    @Before
    public void setUp() {
        executor = new GeaFlowGremlinTraversalExecutor();
    }

    @Test
    public void testExecutorInitialization() {
        // Test that the executor can be instantiated
        Assert.assertNotNull(executor);
    }
    
    @Test
    public void testExecuteMethodExists() {
        // Test that the execute method exists and can be called
        RelNode mockRelNode = Mockito.mock(RelNode.class);
        TraversalRuntimeContext mockContext = Mockito.mock(TraversalRuntimeContext.class);
        
        // This should not throw an exception
        VertexCentricTraversal traversal = executor.execute(mockRelNode, mockContext);
        
        // The result should not be null
        Assert.assertNotNull(traversal);
    }
}