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

import org.apache.geaflow.api.function.base.FlatMapFunction;
import org.apache.geaflow.collector.CollectionCollector;
import org.apache.geaflow.operator.base.window.AbstractOneInputOperator;

public class FlatMapOperator<IN, OUT> extends
    AbstractOneInputOperator<IN, FlatMapFunction<IN, OUT>> {

    private CollectionCollector collectionCollector;

    public FlatMapOperator(FlatMapFunction<IN, OUT> function) {
        super(function);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.collectionCollector = new CollectionCollector(opArgs.getOpId(), this.collectors);
    }

    @Override
    protected void process(IN value) throws Exception {
        function.flatMap(value, collectionCollector);
    }

}
