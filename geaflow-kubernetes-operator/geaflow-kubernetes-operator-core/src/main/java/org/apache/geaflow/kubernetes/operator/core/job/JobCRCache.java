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

package org.apache.geaflow.kubernetes.operator.core.job;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import org.apache.geaflow.kubernetes.operator.core.util.KubernetesUtil;
import org.springframework.stereotype.Component;

@Component
public class JobCRCache {

    private static final Cache<String, GeaflowJob> JOB_CR_CACHE =
        CacheBuilder.newBuilder().initialCapacity(10)
            .maximumSize(2000)
            .build();

    public void updateJobCache(GeaflowJob geaflowJob) {
        JOB_CR_CACHE.put(geaflowJob.appId(), geaflowJob);
    }

    public void invalidateCache(String key) {
        JOB_CR_CACHE.invalidate(key);
    }

    public void cleanup() {
        JOB_CR_CACHE.cleanUp();
    }

    public void updateAll() {
        Collection<GeaflowJob> jobs = KubernetesUtil.getGeaflowJobs(true);
        JOB_CR_CACHE.putAll(jobs.stream().collect(Collectors.toMap(GeaflowJob::appId, job -> job)));
    }

    public Collection<GeaflowJob> getGeaflowJobCRs() {
        return JOB_CR_CACHE.asMap().values();
    }
}
