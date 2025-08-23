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

package org.apache.geaflow.dsl.runtime.testenv;

import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.api.function.GeaFlowTableSinkFunction;
import org.apache.geaflow.dsl.runtime.query.KafkaFoTest;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoGeaFlowTableSinkFunction extends GeaFlowTableSinkFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        FoGeaFlowTableSinkFunction.class);

    public FoGeaFlowTableSinkFunction(GeaFlowTable table, TableSink tableSink) {
        super(table, tableSink);
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        LOGGER.info("open fo sink function.");
        super.open(runtimeContext);
    }

    @Override
    public void write(Row row) throws Exception {
        super.write(row);
        if (KafkaFoTest.injectExceptionTimes > 0) {
            --KafkaFoTest.injectExceptionTimes;
            throw new RuntimeException("break for fo test");
        }
    }
}
