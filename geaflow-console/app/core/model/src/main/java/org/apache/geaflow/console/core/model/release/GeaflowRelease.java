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

package org.apache.geaflow.console.core.model.release;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;

@Getter
@Setter
public class GeaflowRelease extends GeaflowId {

    private GeaflowConfig jobConfig = new GeaflowConfig();
    private GeaflowConfig clusterConfig = new GeaflowConfig();
    private GeaflowJob job;
    private GeaflowVersion version;
    private JobPlan jobPlan;
    private GeaflowCluster cluster;

    private int releaseVersion;

    private String url;

    private String md5;

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(job, "Invalid job");
        if (!job.isApiJob()) {
            Preconditions.checkNotNull(jobPlan, "Invalid jobPlan");
        }
        Preconditions.checkNotNull(version, "Invalid version");
        Preconditions.checkNotNull(cluster, "Invalid cluster");
        Preconditions.checkArgument(releaseVersion >= 1);
    }

    public void addJobConfig(GeaflowConfig config) {
        this.jobConfig.putAll(config);
    }

    public void addClusterConfig(GeaflowConfig config) {
        this.clusterConfig.putAll(config);
    }
}
