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

package org.apache.geaflow.operator.impl.graph.compute.dynamic;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.ENABLE_DETAIL_METRIC;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.utils.ReflectionUtil;
import org.apache.geaflow.context.AbstractRuntimeContext;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DynamicGraphVertexCentricComputeOpTest {

    @Test
    public void testUpdateWindowId() {
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView("test")
            .withShardNum(1)
            .withBackend(IViewDesc.BackendType.RocksDB)
            .withSchema(new GraphMetaType<>(IntegerType.INSTANCE, ValueVertex.class,
                Integer.class, ValueEdge.class, IntegerType.class))
            .build();

        DynamicGraphVertexCentricComputeOp operator = new DynamicGraphVertexCentricComputeOp(
            graphViewDesc, new IncVertexCentricCompute(5) {
            @Override
            public IncVertexCentricComputeFunction getIncComputeFunction() {
                return mock(IncVertexCentricComputeFunction.class);
            }

            @Override
            public VertexCentricCombineFunction getCombineFunction() {
                return null;
            }
        });
        ((AbstractOperator) operator).getOpArgs().setOpName("test");
        ((AbstractOperator) operator).getOpArgs()
            .setConfig(new HashMap<String, String>() {{
                put(ENABLE_DETAIL_METRIC.getKey(), "false");
            }});

        List<ICollector> collectors = new ArrayList<>();
        ICollector collector = mock(ICollector.class);
        when(collector.getId()).thenReturn(0);
        when(collector.getTag()).thenReturn("tag");

        collectors.add(collector);
        collectors.add(collector);
        long startWindowId = 0;
        Operator.OpContext context = new AbstractOperator.DefaultOpContext(collectors,
            new TestRuntimeContext());
        operator.open(context);

        Assert.assertEquals(startWindowId, ReflectionUtil.getField(operator, "windowId"));

        operator.initIteration(2);
        Assert.assertEquals(startWindowId, ReflectionUtil.getField(operator, "windowId"));
        Assert.assertEquals(startWindowId, ((RuntimeContext) ReflectionUtil.getField(operator, "runtimeContext")).getWindowId());

        ((AbstractRuntimeContext) context.getRuntimeContext()).updateWindowId(3L);
        operator.initIteration(1);

        Assert.assertEquals(3L, ReflectionUtil.getField(operator, "windowId"));
        Assert.assertEquals(3L, ((RuntimeContext) ReflectionUtil.getField(operator, "runtimeContext")).getWindowId());
    }

    public class TestRuntimeContext extends AbstractRuntimeContext {

        public TestRuntimeContext() {
            super(new Configuration());
        }

        public TestRuntimeContext(Map<String, String> opConfig) {
            super(new Configuration(opConfig));
        }

        @Override
        public long getPipelineId() {
            return 0;
        }

        @Override
        public String getPipelineName() {
            return null;
        }

        @Override
        public TaskArgs getTaskArgs() {
            return new TaskArgs(0, 0, "test", 1, 1, 0);
        }

        @Override
        public Configuration getConfiguration() {
            return jobConfig;
        }

        @Override
        public RuntimeContext clone(Map<String, String> opConfig) {
            return new TestRuntimeContext(opConfig);
        }

        @Override
        public long getWindowId() {
            return windowId;
        }

        @Override
        public MetricGroup getMetric() {
            Configuration config = new Configuration();
            config.put(ExecutionConfigKeys.REPORTER_LIST.getKey(), "");
            return MetricGroupRegistry.getInstance(config).getMetricGroup();
        }

    }
}
