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

package org.apache.geaflow.store.memory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.IDEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.property.EmptyProperty;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.IDVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.StatePushDown;
import org.apache.geaflow.state.schema.GraphDataSchema;
import org.apache.geaflow.store.api.graph.IStaticGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.log4j.PropertyConfigurator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Fork(1)
@Threads(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Measurement(iterations = 10, time = 1)
@Warmup(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class IntMapGraphJMH {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntMapGraphJMH.class);

    IStatePushDown pushdown = StatePushDown.of();
    IStaticGraphStore<Integer, Object, Object> store;
    StoreContext storeContext = new StoreContext("test").withDataSchema(
        new GraphDataSchema(new GraphMeta(
            new GraphMetaType(IntegerType.INSTANCE, IDVertex.class, IDVertex::new, EmptyProperty.class,
                IDEdge.class, IDEdge::new, EmptyProperty.class))));

    @Setup
    public void setUp() {
        Properties prop = new Properties();
        prop.setProperty("log4j.rootLogger", "INFO, stdout");
        prop.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        prop.setProperty("log4j.appender.stdout.Target", "System.out");
        prop.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.stdout.layout.ConversionPattern",
            "%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p %c{1}:%L - %m%n");
        PropertyConfigurator.configure(prop);

        store = new StaticGraphMemoryStore<Integer, Object, Object>();
        composeGraph();
    }

    @Benchmark
    public void composeGraph() {
        store.init(storeContext);
        for (int i = 0; i < 1000000; i++) {
            IDVertex<Integer> vertex = new IDVertex<>(i);
            store.addVertex(vertex);
            IDEdge<Integer> edge = new IDEdge<>(i, i + 1);
            edge.setDirect(EdgeDirection.IN);
            store.addEdge(edge);
        }
        store.flush();
    }

    @Benchmark
    public void getVertex() {
        for (int i = 0; i < 100000; i++) {
            store.getVertex(i * 10, pushdown);
        }
    }

    @Benchmark
    public void getEdges() {
        for (int i = 0; i < 100000; i++) {
            store.getEdges(i * 10, pushdown);
        }
    }

    @Benchmark
    public void getOneGraph() {
        for (int i = 0; i < 100000; i++) {
            store.getOneDegreeGraph(i * 10, pushdown);
        }
    }

    @Benchmark
    public void getVertexIterator() {
        Iterator<IVertex<Integer, Object>> it =
            store.getVertexIterator(pushdown);
        while (it.hasNext()) {
            it.next();
        }
    }

    @Benchmark
    public void getEdgeIterator() {
        Iterator<IEdge<Integer, Object>> it =
            store.getEdgeIterator(pushdown);
        while (it.hasNext()) {
            it.next();
        }
    }

    @Benchmark
    public void getOneGraphIterator() {
        Iterator<OneDegreeGraph<Integer, Object, Object>> it =
            store.getOneDegreeGraphIterator(
                pushdown);
        while (it.hasNext()) {
            it.next();
        }
    }

    @Benchmark
    public void memoryUsage() {
        storeContext = null;
        System.gc();
        SleepUtils.sleepSecond(1);
        MemoryMXBean mm = ManagementFactory.getMemoryMXBean();
        LOGGER.info("map(MB): {}", mm.getHeapMemoryUsage().getUsed() / 1024 / 1024);
    }
}
