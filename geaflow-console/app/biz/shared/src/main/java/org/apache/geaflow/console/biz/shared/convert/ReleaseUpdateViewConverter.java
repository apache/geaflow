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

package org.apache.geaflow.console.biz.shared.convert;

import java.util.Objects;
import org.apache.geaflow.console.biz.shared.view.ReleaseUpdateView;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.apache.geaflow.console.core.model.config.ConfigDescFactory;
import org.apache.geaflow.console.core.model.config.ConfigRedactor;
import org.apache.geaflow.console.core.model.job.config.ClusterConfigClass;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.release.ReleaseUpdate;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.springframework.stereotype.Component;

@Component
public class ReleaseUpdateViewConverter {

    public ReleaseUpdate converter(ReleaseUpdateView view, GeaflowVersion version, GeaflowCluster cluster,
                                   GeaflowRelease previous) {
        ReleaseUpdate model = new ReleaseUpdate();
        if (view.getNewJobConfig() != null) {
            model.setNewJobConfig(ConfigRedactor.merge(view.getNewJobConfig(), previous.getJobConfig(),
                ConfigRedactor.jobDesc(previous.getJob().getType())));
        }
        if (view.getNewClusterConfig() != null) {
            boolean sameCluster = cluster == null || previous.getCluster() != null
                && Objects.equals(cluster.getId(), previous.getCluster().getId());
            model.setNewClusterConfig(ConfigRedactor.merge(view.getNewClusterConfig(),
                sameCluster ? previous.getClusterConfig() : null, ConfigDescFactory.getOrRegister(ClusterConfigClass.class)));
        }
        model.setNewParallelisms(view.getNewParallelisms());
        model.setNewVersion(version);
        model.setNewCluster(cluster);
        return model;
    }
}
