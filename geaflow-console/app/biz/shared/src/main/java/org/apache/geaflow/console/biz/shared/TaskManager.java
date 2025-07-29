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

package org.apache.geaflow.console.biz.shared;


import javax.servlet.http.HttpServletResponse;
import org.apache.geaflow.console.biz.shared.view.TaskStartupNotifyView;
import org.apache.geaflow.console.biz.shared.view.TaskView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.TaskSearch;
import org.apache.geaflow.console.common.util.type.GeaflowOperationType;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.metric.GeaflowMetric;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricMeta;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricQueryRequest;
import org.apache.geaflow.console.core.model.runtime.GeaflowCycle;
import org.apache.geaflow.console.core.model.runtime.GeaflowError;
import org.apache.geaflow.console.core.model.runtime.GeaflowOffset;
import org.apache.geaflow.console.core.model.runtime.GeaflowPipeline;
import org.apache.geaflow.console.core.model.task.GeaflowHeartbeatInfo;

public interface TaskManager extends IdManager<TaskView, TaskSearch> {

    TaskView getByJobId(String jobId);

    void operate(String taskId, GeaflowOperationType action);

    GeaflowTaskStatus queryStatus(String taskId, Boolean refresh);

    PageList<GeaflowPipeline> queryPipelines(String taskId);

    PageList<GeaflowCycle> queryCycles(String taskId, String pipelineId);

    PageList<GeaflowError> queryErrors(String taskId);

    PageList<GeaflowMetricMeta> queryMetricMeta(String taskId);

    PageList<GeaflowMetric> queryMetrics(String taskId, GeaflowMetricQueryRequest queryRequest);

    PageList<GeaflowOffset> queryOffsets(String taskId);

    GeaflowHeartbeatInfo queryHeartbeat(String taskId);

    void startupNotify(String taskId, TaskStartupNotifyView requestData);

    void download(String taskId, String path, HttpServletResponse response);

    String getLogs(String taskId);
}
