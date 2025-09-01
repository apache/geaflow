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
import org.apache.geaflow.api.function.io.SourceFunction.SourceContext;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.operator.OpArgs.OpType;
import org.apache.geaflow.operator.base.io.SourceOperator;
import org.apache.geaflow.operator.base.window.AbstractStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowSourceOperator<OUT> extends AbstractStreamOperator<SourceFunction<OUT>> implements
    SourceOperator<OUT, Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowSourceOperator.class);

    protected transient SourceContext<OUT> sourceCxt;
    protected IWindow<OUT> windowFunction;

    public WindowSourceOperator() {
        super();
        opArgs.setOpType(OpType.MULTI_WINDOW_SOURCE);
    }

    public WindowSourceOperator(SourceFunction<OUT> sourceFunction) {
        super(sourceFunction);
        opArgs.setOpType(OpType.MULTI_WINDOW_SOURCE);
    }

    public WindowSourceOperator(SourceFunction<OUT> sourceFunction, IWindow<OUT> windowFunction) {
        this(sourceFunction);
        this.windowFunction = windowFunction;
        if (windowFunction instanceof AllWindow) {
            opArgs.setOpType(OpType.SINGLE_WINDOW_SOURCE);
        } else {
            opArgs.setOpType(OpType.MULTI_WINDOW_SOURCE);
        }
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.sourceCxt = new StreamSourceContext();
        TaskArgs taskArgs = opContext.getRuntimeContext().getTaskArgs();
        this.function.init(taskArgs.getParallelism(), taskArgs.getTaskIndex());
    }

    @Override
    public Boolean emit(long windowId) throws Exception {
        try {
            this.windowFunction.initWindow(windowId);
            return this.function.fetch(this.windowFunction, sourceCxt);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void close() {
        super.close();
        this.function.close();
    }

    class StreamSourceContext implements SourceContext<OUT> {

        public StreamSourceContext() {
        }

        @Override
        public boolean collect(OUT element) throws Exception {
            collectValue(element);
            return true;
        }

    }
}
