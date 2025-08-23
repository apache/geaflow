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

package org.apache.geaflow.console.core.service.store;

import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricMeta;
import org.apache.geaflow.console.core.model.runtime.GeaflowCycle;
import org.apache.geaflow.console.core.model.runtime.GeaflowError;
import org.apache.geaflow.console.core.model.runtime.GeaflowOffset;
import org.apache.geaflow.console.core.model.runtime.GeaflowPipeline;
import org.apache.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import org.apache.geaflow.console.core.model.task.GeaflowTask;

public interface GeaflowRuntimeMetaStore {

    PageList<GeaflowPipeline> queryPipelines(GeaflowTask task);

    PageList<GeaflowCycle> queryCycles(GeaflowTask task, String pipelineId);

    PageList<GeaflowOffset> queryOffsets(GeaflowTask task);

    PageList<GeaflowError> queryErrors(GeaflowTask task);

    PageList<GeaflowMetricMeta> queryMetricMeta(GeaflowTask task);

    GeaflowHeartbeatInfo queryHeartbeat(GeaflowTask task);

    void cleanRuntimeMeta(GeaflowTask task);

}
