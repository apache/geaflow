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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTwoInputOperator<T, U, FUNC extends Function> extends
    AbstractStreamOperator<FUNC> implements TwoInputOperator<T, U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTwoInputOperator.class);

    public AbstractTwoInputOperator() {
        super();
    }

    public AbstractTwoInputOperator(FUNC func) {
        super(func);
    }

    @Override
    public void processElementOne(T value) throws Exception {
        this.ticToc.ticNano();
        processRecordOne(value);
        this.opRtHistogram.update(this.ticToc.tocNano() / 1000);
        this.opInputMeter.mark();
    }

    @Override
    public void processElementTwo(U value) throws Exception {
        this.ticToc.ticNano();
        processRecordTwo(value);
        this.opRtHistogram.update(this.ticToc.tocNano() / 1000);
        this.opInputMeter.mark();
    }

    protected abstract void processRecordOne(T value) throws Exception;

    protected abstract void processRecordTwo(U value) throws Exception;

}
