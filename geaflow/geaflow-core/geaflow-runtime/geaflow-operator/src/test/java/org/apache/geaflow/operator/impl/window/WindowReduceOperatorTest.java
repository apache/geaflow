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

package org.apache.geaflow.operator.impl.window;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.base.ReduceFunction;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WindowReduceOperatorTest {

    private static int batchSize = 10;

    private WindowReduceOperator operator;
    private MyReduce reduce;
    private Operator.OpContext opContext;
    private Map<Long, Long> batchId2Value;

    @BeforeMethod
    public void setup() {
        ICollector collector = mock(ICollector.class);
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        when(runtimeContext.getConfiguration()).thenReturn(new Configuration());
        when(runtimeContext.getTaskArgs()).thenReturn(new TaskArgs(1, 0, "agg", 1, 1024, 0));
        when(runtimeContext.clone(any(Map.class))).thenReturn(runtimeContext);
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.REPORTER_LIST.getKey(), "");
        MetricGroup metricGroup = MetricGroupRegistry.getInstance(config).getMetricGroup();
        Mockito.doReturn(metricGroup).when(runtimeContext).getMetric();
        Mockito.doReturn(runtimeContext).when(runtimeContext).clone(any());
        this.opContext = new AbstractOperator.DefaultOpContext(
            Lists.newArrayList(collector), runtimeContext);
        this.reduce = new MyReduce();
        this.operator = new WindowReduceOperator(this.reduce, new KeySelectorFunc());
        this.operator.open(opContext);

        this.batchId2Value = new HashMap<>();
    }

    @Test
    public void testReduce() throws Exception {
        long batchId = 1;
        long value = 0;
        for (int i = 0; i < 1000; i++) {
            value += i;
            if ((i + 1) % batchSize == 0) {
                this.batchId2Value.put(batchId++, value);
                value = 0;
            }
        }

        batchId = 1;
        for (int i = 0; i < 1000; i++) {
            this.operator.processValue(i);
            if ((i + 1) % batchSize == 0) {
                this.operator.finish();
                batchId++;
                Assert.assertTrue(this.reduce.getValue() == this.batchId2Value.get(batchId - 1));
                this.reduce.setValue(i + 1);
            }
        }
    }

    public static class KeySelectorFunc implements KeySelector<Integer, Integer> {

        @Override
        public Integer getKey(Integer value) {
            return 0;
        }
    }

    class MyReduce implements ReduceFunction<Integer> {

        private int value;

        public MyReduce() {
            this.value = 0;
        }

        @Override
        public Integer reduce(Integer oldValue, Integer newValue) {
            value += newValue;
            return oldValue + newValue;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }

}
