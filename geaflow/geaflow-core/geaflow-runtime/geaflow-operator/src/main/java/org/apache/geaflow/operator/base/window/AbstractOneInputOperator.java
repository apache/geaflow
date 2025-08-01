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

package org.apache.geaflow.operator.base.window;

import org.apache.geaflow.api.function.Function;
import org.apache.geaflow.operator.OpArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOneInputOperator<T, FUNC extends Function> extends
    AbstractStreamOperator<FUNC> implements OneInputOperator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOneInputOperator.class);

    public AbstractOneInputOperator() {
        super();
        opArgs.setOpType(OpArgs.OpType.ONE_INPUT);
    }

    public AbstractOneInputOperator(FUNC func) {
        super(func);
        opArgs.setOpType(OpArgs.OpType.ONE_INPUT);
    }

    @Override
    public void processElement(T value) throws Exception {
        this.ticToc.ticNano();
        process(value);
        this.opRtHistogram.update(this.ticToc.tocNano() / 1000);
        this.opInputMeter.mark();
    }

    protected abstract void process(T value) throws Exception;

}
