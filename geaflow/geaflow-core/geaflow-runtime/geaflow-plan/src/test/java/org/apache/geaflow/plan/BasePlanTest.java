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

package org.apache.geaflow.plan;

import org.apache.geaflow.operator.impl.io.WindowSourceOperator;
import org.apache.geaflow.operator.impl.window.KeySelectorOperator;
import org.apache.geaflow.operator.impl.window.MapOperator;
import org.apache.geaflow.operator.impl.window.SinkOperator;
import org.apache.geaflow.partitioner.impl.ForwardPartitioner;
import org.apache.geaflow.partitioner.impl.KeyPartitioner;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;

public class BasePlanTest {

    protected PipelineGraph plan;

    public void setUp() {
        plan = new PipelineGraph();
        Class<?>[] typeArguments = new Class[4];
        typeArguments[0] = Integer.class;
        typeArguments[1] = Integer.class;
        typeArguments[2] = Integer.class;
        typeArguments[3] = Integer.class;

        plan.addVertex(new PipelineVertex(1, new WindowSourceOperator<>(null, null), 1));
        plan.addVertex(new PipelineVertex(2, new MapOperator<>(null), 1));

        MapOperator mapOperator = new MapOperator(null);
        mapOperator.getOpArgs().setParallelism(7);
        plan.addVertex(new PipelineVertex(3, mapOperator, 1));
        plan.addVertex(new PipelineVertex(4, new SinkOperator<>(null), 1));
        plan.addVertex(new PipelineVertex(5, new KeySelectorOperator<>(null), 1));
        KeySelectorOperator keySelectorOperator = new KeySelectorOperator<>(null);
        keySelectorOperator.getOpArgs().setParallelism(7);
        plan.addVertex(new PipelineVertex(6, keySelectorOperator, 1));
        plan.addVertex(new PipelineVertex(7, new SinkOperator<>(null), 1));

        plan.addEdge(new PipelineEdge(1, 2, 4, new ForwardPartitioner(), null));
        plan.addEdge(new PipelineEdge(2, 1, 2, new ForwardPartitioner(), null));
        plan.addEdge(new PipelineEdge(3, 5, 7, new KeyPartitioner(1), null));
        plan.addEdge(new PipelineEdge(4, 6, 7, new KeyPartitioner(2), null));
        plan.addEdge(new PipelineEdge(5, 2, 5, new ForwardPartitioner(), null));
        plan.addEdge(new PipelineEdge(6, 3, 6, new ForwardPartitioner(), null));
        plan.addEdge(new PipelineEdge(7, 1, 3, new ForwardPartitioner(), null));
    }
}
