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

package org.apache.geaflow.operator.base;

import static org.mockito.Matchers.any;

import java.util.ArrayList;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.window.OneInputOperator;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AbstractOperatorTest {

    @Test
    public void testChainedOperator() {

        TestFunction function = new TestFunction();
        AbstractOperator operator = new TestOperator(function);
        TestFunction subFunction = new TestFunction();
        AbstractOperator subOperator = new TestOperator(subFunction);
        operator.addNextOperator(subOperator);

        RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.REPORTER_LIST.getKey(), "");
        MetricGroup metricGroup = MetricGroupRegistry.getInstance(config).getMetricGroup();
        Mockito.doReturn(metricGroup).when(runtimeContext).getMetric();
        Mockito.doReturn(runtimeContext).when(runtimeContext).clone(any());
        Mockito.doReturn(config).when(runtimeContext).getConfiguration();

        Operator.OpContext opContext = new AbstractOperator.DefaultOpContext(new ArrayList<>(), runtimeContext);
        operator.open(opContext);

        Assert.assertTrue(function.isOpened());
        Assert.assertTrue(subFunction.isOpened());

        operator.close();
        Assert.assertTrue(function.isClosed());
        Assert.assertTrue(subFunction.isClosed());
    }

    private class TestOperator extends AbstractOperator<TestFunction> implements OneInputOperator<TestFunction> {

        public TestOperator(TestFunction function) {
            super(function);
        }

        @Override
        public void processElement(TestFunction value) {
        }
    }

    private class TestFunction extends RichFunction {

        private boolean opened;
        private boolean closed;

        @Override
        public void open(RuntimeContext runtimeContext) {
            this.opened = true;
        }

        @Override
        public void close() {
            this.closed = true;
        }

        public boolean isOpened() {
            return opened;
        }

        public boolean isClosed() {
            return closed;
        }
    }

}
