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
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStreamOperator<FUNC extends Function> extends AbstractOperator<FUNC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamOperator.class);

    public AbstractStreamOperator() {
        super();
    }

    public AbstractStreamOperator(FUNC func) {
        super(func);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
    }

    protected final <T> void collectValue(T value) {
        if (value == null) {
            return;
        }
        for (int i = 0, size = collectors.size(); i < size; i++) {
            ICollector collector = this.collectors.get(i);
            collector.partition(value);
        }
    }

    protected final <KEY, VALUE> void collectKValue(KEY key, VALUE value) {
        for (int i = 0, size = collectors.size(); i < size; i++) {
            ICollector collector = this.collectors.get(i);
            collector.partition(key, value);
        }
    }

}
