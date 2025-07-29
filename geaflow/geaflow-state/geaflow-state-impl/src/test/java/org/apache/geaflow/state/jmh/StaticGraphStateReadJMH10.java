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

package org.apache.geaflow.state.jmh;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@Threads(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 3, time = 1)
@State(Scope.Benchmark)
public class StaticGraphStateReadJMH10 extends JMHParameter {

    GraphState<Integer, Integer, Integer> graphState;

    @Setup(Level.Trial)
    public void setUp() {
        composeGraph();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    public void composeGraph() {
        GraphStateDescriptor<Integer, Integer, Integer> desc = GraphStateDescriptor.build(
            "StaticGraphStateJMH", storeType);
        GraphMetaType tag = new GraphMetaType(Types.INTEGER, ValueVertex.class,
            Integer.class, ValueLabelTimeEdge.class, Integer.class);
        desc.withGraphMeta(new GraphMeta(tag)).withKeyGroup(new KeyGroup(0, 0));
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), getClass().getSimpleName());
        graphState = StateFactory.buildGraphState(desc, configuration);

        graphState.manage().operate().setCheckpointId(1);
        for (int i = 0; i < vNum; i++) {
            IVertex<Integer, Integer> vertex = new ValueVertex<>(i, i);
            graphState.staticGraph().V().add(vertex);
            for (int j = 1; j < outE; j++) {
                IEdge<Integer, Integer> edge = new ValueLabelTimeEdge<>(i, j, i,
                    i % 2 == 0 ? EdgeDirection.IN : EdgeDirection.OUT,
                    Integer.toString(i % 10), i + 10000000);
                graphState.staticGraph().E().add(edge);
            }
        }
        graphState.manage().operate().finish();
    }

    @Benchmark
    public void getVertex(Blackhole blackhole) {
        for (int i = 0; i < vNum; i++) {
            blackhole.consume(graphState.staticGraph().V().query(i).get());
        }
    }

    @Benchmark
    public void getEdges(Blackhole blackhole) {
        for (int i = 0; i < vNum; i++) {
            blackhole.consume(graphState.staticGraph().E().query(i).asList());
        }
    }

    @Benchmark
    public void getOneGraph(Blackhole blackhole) {
        for (int i = 0; i < vNum; i++) {
            blackhole.consume(graphState.staticGraph().VE().query(i).get());
        }
    }

    @Benchmark
    public void getVertexIterator(Blackhole blackhole) {
        Iterator<IVertex<Integer, Integer>> it = graphState.staticGraph().V().iterator();
        while (it.hasNext()) {
            blackhole.consume(it.next());
        }
    }

    //@Benchmark
    public void getEdgeIterator(Blackhole blackhole) {
        Iterator<IEdge<Integer, Integer>> it = graphState.staticGraph().E().iterator();
        while (it.hasNext()) {
            blackhole.consume(it.next());
        }
    }

    @Benchmark
    public void getOneGraphIterator(Blackhole blackhole) {
        Iterator<OneDegreeGraph<Integer, Integer, Integer>> it =
            graphState.staticGraph().VE().iterator();
        while (it.hasNext()) {
            OneDegreeGraph<Integer, Integer, Integer> next = it.next();
            blackhole.consume(next);
        }
    }
}
