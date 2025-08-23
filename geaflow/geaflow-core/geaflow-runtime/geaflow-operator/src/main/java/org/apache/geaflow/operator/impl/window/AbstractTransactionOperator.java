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
import org.apache.geaflow.operator.base.window.AbstractOneInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTransactionOperator<T> extends AbstractOneInputOperator<T, SinkFunction<T>> implements
    TransactionTrait {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(AbstractTransactionOperator.class);

    public AbstractTransactionOperator() {
        super();
    }

    public AbstractTransactionOperator(SinkFunction func) {
        super(func);
    }

    @Override
    public void finish(long windowId) {
        LOGGER.info("transaction operator finish batch:{}", windowId);
    }

    @Override
    public void rollback(long windowId) {
        LOGGER.info("transaction operator rollback batch:{}", windowId);
    }
}
