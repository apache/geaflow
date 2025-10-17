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
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.model.graph.message.IGraphMessage;

/**
 * Implementation of GremlinTraversalExecutor for GeaFlow.
 */
public class GeaFlowGremlinTraversalExecutor implements GremlinTraversalExecutor {

    @Override
    public VertexCentricTraversal execute(RelNode relNode, TraversalRuntimeContext traversalContext) {
        // Convert the RelNode to a VertexCentricTraversal
        // This is where we would implement the logic to translate the RelNode
        // into a VertexCentricTraversal that can be executed by GeaFlow
        
        // Create and return a VertexCentricTraversal with the GremlinVertexProgram
        return new VertexCentricTraversal<Object, Object, Object, IGraphMessage, Object>(10) {
            @Override
            public org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction<Object, Object, Object, IGraphMessage, Object> getTraversalFunction() {
                return new GremlinVertexProgram();
            }
            
            @Override
            public VertexCentricCombineFunction<IGraphMessage> getCombineFunction() {
                // Return null for now, as we don't need a combine function for Gremlin traversals
                return null;
            }
        };
    }
}