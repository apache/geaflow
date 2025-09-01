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

import org.apache.geaflow.console.biz.shared.AuthorizationManager;
import org.apache.geaflow.console.biz.shared.ClusterManager;
import org.apache.geaflow.console.biz.shared.view.ClusterView;
import org.apache.geaflow.console.common.dal.model.ClusterSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowOperationType;
import org.apache.geaflow.console.core.model.security.GeaflowRole;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/clusters")
public class ClusterController {

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping
    public GeaflowApiResponse<PageList<ClusterView>> searchClusters(ClusterSearch search) {
        return GeaflowApiResponse.success(clusterManager.search(search));
    }

    @GetMapping("/{clusterName}")
    public GeaflowApiResponse<ClusterView> queryCluster(@PathVariable String clusterName) {
        return GeaflowApiResponse.success(clusterManager.getByName(clusterName));
    }

    @PostMapping
    public GeaflowApiResponse<String> createCluster(@RequestBody ClusterView clusterView) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(clusterManager.create(clusterView));
    }

    @PutMapping("/{clusterName}")
    public GeaflowApiResponse<Boolean> updateCluster(@PathVariable String clusterName,
                                                     @RequestBody ClusterView clusterView) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(clusterManager.updateByName(clusterName, clusterView));
    }

    @DeleteMapping("/{clusterName}")
    public GeaflowApiResponse<Boolean> deleteCluster(@PathVariable String clusterName) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(clusterManager.dropByName(clusterName));
    }

    @PostMapping("/{clusterName}/operations")
    public GeaflowApiResponse<Boolean> operateCluster(@PathVariable String clusterName,
                                                      @RequestParam GeaflowOperationType clusterAction) {

        throw new GeaflowException("Cluster operation not supported");
    }

}
