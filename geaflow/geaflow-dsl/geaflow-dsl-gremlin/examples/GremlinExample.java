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

package org.apache.geaflow.dsl.gremlin.examples;

import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.dsl.gremlin.parser.GeaFlowGremlinParser;
import org.apache.geaflow.dsl.gremlin.parser.GremlinQuery;

import org.apache.geaflow.dsl.gremlin.plan.converter.GeaFlowGremlinToRelConverter;
import org.apache.geaflow.dsl.gremlin.runtime.adapter.GeaFlowGremlinTraversalExecutor;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.plan.PhysicRelNode;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * Example of how to use the GeaFlow Gremlin support.
 */
public class GremlinExample {

    public static void main(String[] args) {
        // Create a graph instance
        Graph graph = TinkerGraph.open();
        
        // Create the Gremlin parser
        GeaFlowGremlinParser parser = new GeaFlowGremlinParser(graph);
        
        // Parse a Gremlin query
        String gremlinQuery = "g.V().out('knows').has('age', gt(30)).values('name')";
        GremlinQuery query = parser.parse(gremlinQuery);
    
        // 2. 转换为 GeaFlow 统一 IR (RelNode)
        GeaFlowGremlinToRelConverter converter = new GeaFlowGremlinToRelConverter();
        RelNode relNode = converter.convert(query, gqlToRelConverter);
    
        // 3. 优化 RelNode
        RelNode optimizedRelNode = optimizer.optimize(relNode);
    
        // 4. 转换为 Physical Plan
        PhysicRelNode physicalPlan = planner.toPhysical(optimizedRelNode);
    
        // 5. 转换为 VertexCentricTraversal
        VertexCentricTraversal traversal = executor.execute(physicalPlan, context);
    
        // 6. 执行并获取结果
        PWindowStream<ITraversalResponse<R>> results = graph.traversalOnVertexCentric(traversal);
        List<R> finalResults = results.collect();
    }
    
}