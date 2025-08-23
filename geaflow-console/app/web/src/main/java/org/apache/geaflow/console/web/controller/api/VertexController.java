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

import org.apache.geaflow.console.biz.shared.VertexManager;
import org.apache.geaflow.console.biz.shared.view.VertexView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.VertexSearch;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class VertexController {

    @Autowired
    private VertexManager vertexManager;

    @GetMapping("/vertices")
    public GeaflowApiResponse<PageList<VertexView>> search(VertexSearch search) {
        return GeaflowApiResponse.success(vertexManager.search(search));
    }

    @GetMapping("/instances/{instanceName}/vertices")
    public GeaflowApiResponse<PageList<VertexView>> instanceSearch(@PathVariable("instanceName") String instanceName,
                                                                   VertexSearch search) {
        return GeaflowApiResponse.success(vertexManager.searchByInstanceName(instanceName, search));
    }

    @GetMapping("/instances/{instanceName}/vertices/{vertexName}")
    public GeaflowApiResponse<VertexView> getVertex(@PathVariable("instanceName") String instanceName,
                                                    @PathVariable("vertexName") String vertexName) {
        return GeaflowApiResponse.success(vertexManager.getByName(instanceName, vertexName));
    }

    @PostMapping("/instances/{instanceName}/vertices")
    public GeaflowApiResponse<String> create(@PathVariable("instanceName") String instanceName,
                                             @RequestBody VertexView vertexView) {
        return GeaflowApiResponse.success(vertexManager.create(instanceName, vertexView));
    }

    @PutMapping("/instances/{instanceName}/vertices/{vertexName}")
    public GeaflowApiResponse<Boolean> update(@PathVariable("instanceName") String instanceName,
                                              @PathVariable("vertexName") String vertexName,
                                              @RequestBody VertexView vertexView) {
        return GeaflowApiResponse.success(vertexManager.updateByName(instanceName, vertexName, vertexView));
    }


    @DeleteMapping("/instances/{instanceName}/vertices/{vertexName}")
    public GeaflowApiResponse<Boolean> drop(@PathVariable("instanceName") String instanceName,
                                            @PathVariable("vertexName") String vertexName) {

        return GeaflowApiResponse.success(vertexManager.dropByName(instanceName, vertexName));
    }
}
