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

import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.job.config.CodeJobConfigClass;
import org.apache.geaflow.console.core.model.job.config.JobConfigClass;
import org.apache.geaflow.console.core.model.job.config.ServeJobConfigClass;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JobConfigBuilder {

    public GeaflowConfig buildDefaultConfig(GeaflowRelease release) {
        JobConfigClass configClass;
        GeaflowJob job = release.getJob();
        switch (job.getType()) {
            case SERVE:
                configClass = initServeJobConfigClass(release);
                break;
            case INTEGRATE:
                configClass = new CodeJobConfigClass();
                ((CodeJobConfigClass) configClass).setWindowSize(-1);
                break;
            default:
                configClass = new JobConfigClass();
                break;
        }

        return configClass.build();
    }

    private JobConfigClass initServeJobConfigClass(GeaflowRelease release) {
        GeaflowJob job = release.getJob();
        GeaflowGraph graph = job.getGraphs().get(0);

        ServeJobConfigClass configClass = new ServeJobConfigClass();
        configClass.setJobMode("OLAP_SERVICE");
        configClass.setServiceShareEnable(true);
        configClass.setGraphName(graph.getName());

        int shardCount = graph.getShardCount();
        int driverNum = 1;
        int queryParallelism = shardCount % driverNum == 0 ? shardCount / driverNum :
            shardCount / driverNum + 1;
        configClass.setQueryParallelism(queryParallelism);
        configClass.setDriverNum(driverNum);
        return configClass;
    }
}
