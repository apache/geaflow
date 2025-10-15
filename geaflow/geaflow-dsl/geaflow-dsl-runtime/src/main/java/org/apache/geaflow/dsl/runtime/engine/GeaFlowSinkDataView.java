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

package org.apache.geaflow.dsl.runtime.engine;

import java.util.List;
import org.apache.geaflow.api.pdata.PStreamSink;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.runtime.SinkDataView;
import org.apache.geaflow.pipeline.job.IPipelineJobContext;

public class GeaFlowSinkDataView implements SinkDataView {

    private final IPipelineJobContext context;

    private final PStreamSink<Row> sink;

    public GeaFlowSinkDataView(IPipelineJobContext context, PStreamSink<Row> sink) {
        this.context = context;
        this.sink = sink;
    }

    @Override
    public <T> T getPlan() {
        return (T) sink;
    }

    @Override
    public List<? extends Row> take(IType<?> type) {
        throw new GeaFlowDSLException("Should not call take() on SinkDataView");
    }
}
