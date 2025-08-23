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

package org.apache.geaflow.kubernetes.operator.web.controller.api;

import java.util.Collection;
import org.apache.geaflow.kubernetes.operator.core.job.GeaflowJobApiService;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import org.apache.geaflow.kubernetes.operator.web.api.ApiResponse;
import org.apache.geaflow.kubernetes.operator.web.converter.GeaflowJobConverter;
import org.apache.geaflow.kubernetes.operator.web.view.GeaflowJobView;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private GeaflowJobApiService geaflowJobApiService;

    @GetMapping
    public ApiResponse<Collection<GeaflowJob>> queryGeaflowJobs() {
        return ApiResponse.success(geaflowJobApiService.queryJobs());
    }

    @PostMapping
    public ApiResponse<Void> createGeaflowJob(@RequestBody GeaflowJobView geaflowJobView) {
        GeaflowJob geaflowJob = GeaflowJobConverter.convert2GeaflowJob(geaflowJobView);
        geaflowJobApiService.createJob(geaflowJob);
        return ApiResponse.success(null);
    }

    @DeleteMapping(path = "/{name}")
    public ApiResponse<Void> deleteGeaflowJob(@PathVariable("name") String name) {
        geaflowJobApiService.deleteJob(name);
        return ApiResponse.success(null);
    }


}
