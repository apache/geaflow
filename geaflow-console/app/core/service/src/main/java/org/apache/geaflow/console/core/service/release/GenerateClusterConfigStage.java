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

import org.apache.commons.collections.MapUtils;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.release.ReleaseUpdate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GenerateClusterConfigStage extends GeaflowBuildStage {

    @Autowired
    private ClusterConfigBuilder clusterConfigBuilder;

    @Override
    public void init(GeaflowRelease release) {
        // generate default cluster config from job config
        GeaflowConfig clusterConfig = clusterConfigBuilder.buildDefaultConfig(release);

        if (release.getClusterConfig() != null) {
            clusterConfig.putAll(release.getClusterConfig());
        }

        release.setClusterConfig(clusterConfig);
    }

    @Override
    public boolean update(GeaflowRelease release, ReleaseUpdate update) {
        GeaflowConfig newClusterConfig = update.getNewClusterConfig();
        if (MapUtils.isEmpty(newClusterConfig)) {
            return false;
        }

        release.setClusterConfig(newClusterConfig);
        return true;
    }
}
