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

package org.apache.geaflow.console.web.controller.api;

import java.util.List;
import org.apache.geaflow.console.biz.shared.AuthorizationManager;
import org.apache.geaflow.console.biz.shared.JobManager;
import org.apache.geaflow.console.biz.shared.view.JobView;
import org.apache.geaflow.console.common.dal.model.JobSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.core.model.security.GeaflowAuthority;
import org.apache.geaflow.console.core.service.security.Resources;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private JobManager jobManager;

    @Autowired
    private AuthorizationManager authorizationManager;


    @GetMapping
    public GeaflowApiResponse<PageList<JobView>> searchJob(JobSearch search) {
        return GeaflowApiResponse.success(jobManager.search(search));
    }

    @GetMapping("/{jobId}")
    public GeaflowApiResponse<JobView> getJob(@PathVariable String jobId) {
        authorizationManager.hasAuthority(GeaflowAuthority.QUERY, Resources.job(jobId));
        return GeaflowApiResponse.success(jobManager.get(jobId));
    }

    @PostMapping
    public GeaflowApiResponse<String> createJob(JobView jobView,
                                                @RequestParam(required = false) MultipartFile jarFile,
                                                @RequestParam(required = false) String fileId,
                                                @RequestParam(required = false) List<String> graphIds) {
        authorizationManager.hasAuthority(GeaflowAuthority.ALL, Resources.instance(jobView.getInstanceId()));
        return GeaflowApiResponse.success(jobManager.create(jobView, jarFile, fileId, graphIds));
    }

    @PutMapping("/{jobId}")
    public GeaflowApiResponse<Boolean> updateJob(@PathVariable String jobId, JobView jobView,
                                                 @RequestParam(required = false) MultipartFile jarFile,
                                                 @RequestParam(required = false) String fileId) {
        authorizationManager.hasAuthority(GeaflowAuthority.UPDATE, Resources.job(jobId));
        return GeaflowApiResponse.success(jobManager.update(jobId, jobView, jarFile, fileId));
    }

    @DeleteMapping("/{jobId}")
    public GeaflowApiResponse<Boolean> deleteJob(@PathVariable String jobId) {
        authorizationManager.hasAuthority(GeaflowAuthority.ALL, Resources.job(jobId));
        return GeaflowApiResponse.success(jobManager.drop(jobId));
    }

}
