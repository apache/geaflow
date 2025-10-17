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

import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import org.apache.geaflow.model.graph.message.IGraphMessage;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Iterator;

/**
 * Unit tests for GremlinVertexProgram.
 */
public class GremlinVertexProgramTest {

    private GremlinVertexProgram vertexProgram;

    @Before
    public void setUp() {
        vertexProgram = new GremlinVertexProgram();
    }

    @Test
    public void testVertexProgramInitialization() {
        // Test that the vertex program can be instantiated
        Assert.assertNotNull(vertexProgram);
    }
    
    @Test
    public void testOpenMethod() {
        // Test that the open method exists and can be called
        VertexCentricTraversalFunction.VertexCentricTraversalFuncContext<Object, Object, Object, IGraphMessage, Object> mockContext = 
            Mockito.mock(VertexCentricTraversalFunction.VertexCentricTraversalFuncContext.class);
        
        // This should not throw an exception
        vertexProgram.open(mockContext);
    }
    
    @Test
    public void testInitMethod() {
        // Test that the init method exists and can be called
        ITraversalRequest<Object> mockRequest = Mockito.mock(ITraversalRequest.class);
        
        // This should not throw an exception
        vertexProgram.init(mockRequest);
    }
    
    @Test
    public void testComputeMethod() {
        // Test that the compute method exists and can be called
        Object vertexId = "testVertex";
        Iterator<IGraphMessage> emptyIterator = Collections.emptyIterator();
        
        // This should not throw an exception
        vertexProgram.compute(vertexId, emptyIterator);
    }
    
    @Test
    public void testFinishMethod() {
        // Test that the finish method exists and can be called
        // This should not throw an exception
        vertexProgram.finish();
    }
    
    @Test
    public void testCloseMethod() {
        // Test that the close method exists and can be called
        // This should not throw an exception
        vertexProgram.close();
    }
}