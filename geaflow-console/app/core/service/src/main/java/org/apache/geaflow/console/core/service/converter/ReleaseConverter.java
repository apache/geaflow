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

package org.apache.geaflow.console.core.service.converter;

import com.alibaba.fastjson.JSON;
import org.apache.geaflow.console.common.dal.entity.ReleaseEntity;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.release.JobPlan;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.springframework.stereotype.Component;

@Component
public class ReleaseConverter extends IdConverter<GeaflowRelease, ReleaseEntity> {


    @Override
    public ReleaseEntity modelToEntity(GeaflowRelease model) {
        ReleaseEntity entity = super.modelToEntity(model);
        entity.setClusterConfig(JSON.toJSONString(model.getClusterConfig()));
        entity.setJobConfig(JSON.toJSONString(model.getJobConfig()));
        entity.setClusterId(model.getCluster().getId());
        entity.setJobPlan(JSON.toJSONString(model.getJobPlan()));
        entity.setJobId(model.getJob().getId());
        entity.setVersionId(model.getVersion().getId());
        entity.setVersion(model.getReleaseVersion());
        entity.setUrl(model.getUrl());
        entity.setMd5(model.getMd5());
        return entity;
    }

    public GeaflowRelease convert(ReleaseEntity entity, GeaflowJob job, GeaflowVersion version, GeaflowCluster cluster) {
        GeaflowRelease release = super.entityToModel(entity);
        // job
        release.setJob(job);
        // versionNumber
        release.setReleaseVersion(entity.getVersion());
        // job config
        GeaflowConfig jobConfig = JSON.parseObject(entity.getJobConfig(), GeaflowConfig.class);
        release.getJobConfig().putAll(jobConfig);
        // cluster config
        GeaflowConfig clusterConfig = JSON.parseObject(entity.getClusterConfig(), GeaflowConfig.class);
        release.getClusterConfig().putAll(clusterConfig);
        // build jobPlan
        JobPlan jobPlan = JobPlan.build(entity.getJobPlan());
        release.setJobPlan(jobPlan);
        // version
        release.setVersion(version);
        // cluster
        release.setCluster(cluster);
        release.setUrl(entity.getUrl());
        release.setMd5(entity.getMd5());
        return release;
    }
}
