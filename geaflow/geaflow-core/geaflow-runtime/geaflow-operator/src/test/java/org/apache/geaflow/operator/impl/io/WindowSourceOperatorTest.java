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

package org.apache.geaflow.operator.impl.io;

import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowSourceOperatorTest {

    @Test
    public void testSourceOperator() {

        TestSourceFunction function = new TestSourceFunction();
        AbstractOperator operator = new WindowSourceOperator(function);
        operator.close();
        Assert.assertTrue(function.isClosed());
    }

    private class TestSourceFunction implements SourceFunction {

        private boolean closed;

        @Override
        public void init(int parallel, int index) {
        }

        @Override
        public boolean fetch(IWindow window, SourceContext ctx) throws Exception {
            return false;
        }

        @Override
        public void close() {
            this.closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
