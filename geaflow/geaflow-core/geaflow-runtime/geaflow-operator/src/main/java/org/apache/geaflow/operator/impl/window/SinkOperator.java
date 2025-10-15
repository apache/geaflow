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

import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.trait.TransactionTrait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkOperator<T> extends AbstractTransactionOperator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkOperator.class);

    private boolean isTransactionFunc;

    public SinkOperator() {
        super();
    }

    public SinkOperator(SinkFunction<T> sinkFunction) {
        super(sinkFunction);
        this.isTransactionFunc = sinkFunction instanceof TransactionTrait;
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
    }

    @Override
    protected void process(T value) throws Exception {
        this.function.write(value);
    }

    @Override
    public void finish(long windowId) {
        if (this.isTransactionFunc) {
            ((TransactionTrait) this.function).finish(windowId);
        }
    }

    @Override
    public void rollback(long windowId) {
        if (this.isTransactionFunc) {
            ((TransactionTrait) this.function).rollback(windowId);
        }
    }
}
