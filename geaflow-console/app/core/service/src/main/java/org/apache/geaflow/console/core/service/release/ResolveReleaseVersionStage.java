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

package org.apache.geaflow.console.core.service.release;

import org.apache.geaflow.console.common.util.type.GeaflowOperationType;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.release.ReleaseUpdate;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResolveReleaseVersionStage extends GeaflowBuildStage {

    @Autowired
    private TaskService taskService;

    public void init(GeaflowRelease release) {
        String jobId = release.getJob().getId();

        GeaflowTask task = taskService.getByJobId(jobId);
        if (task == null) {
            // publish at the first time
            release.setReleaseVersion(1);
        } else {
            GeaflowTaskStatus status = task.getStatus();
            status.checkOperation(GeaflowOperationType.PUBLISH);

            GeaflowRelease oldRelease = task.getRelease();
            int currentVersion = oldRelease.getReleaseVersion();
            if (status == GeaflowTaskStatus.CREATED) {
                //update release, releaseVersion unchanged
                release.setReleaseVersion(currentVersion);
                release.setId(task.getRelease().getId());
            } else {
                //stop, fail, finish, versionNumber + 1
                release.setReleaseVersion(currentVersion + 1);
            }

            release.setJobConfig(oldRelease.getJobConfig());
            release.setClusterConfig(oldRelease.getClusterConfig());
        }
    }


    @Override
    public boolean update(GeaflowRelease release, ReleaseUpdate update) {
        return false;
    }
}
