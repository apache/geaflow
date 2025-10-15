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
import org.apache.geaflow.console.biz.shared.GraphManager;
import org.apache.geaflow.console.biz.shared.view.EndpointView;
import org.apache.geaflow.console.biz.shared.view.GraphView;
import org.apache.geaflow.console.common.dal.model.GraphSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GraphController {

    @Autowired
    private GraphManager graphManager;

    @GetMapping("/graphs")
    public GeaflowApiResponse<PageList<GraphView>> search(GraphSearch search) {
        return GeaflowApiResponse.success(graphManager.search(search));
    }

    @GetMapping("/instances/{instanceName}/graphs")
    public GeaflowApiResponse<PageList<GraphView>> instanceSearch(@PathVariable("instanceName") String instanceName,
                                                                  GraphSearch search) {
        return GeaflowApiResponse.success(graphManager.searchByInstanceName(instanceName, search));
    }

    @GetMapping("/instances/{instanceName}/graphs/{graphName}")
    public GeaflowApiResponse<GraphView> getGraph(@PathVariable("instanceName") String instanceName,
                                                  @PathVariable("graphName") String graphName) {
        return GeaflowApiResponse.success(graphManager.getByName(instanceName, graphName));
    }

    @PostMapping("/instances/{instanceName}/graphs")
    public GeaflowApiResponse<String> create(@PathVariable("instanceName") String instanceName,
                                             @RequestBody GraphView graphView) {
        return GeaflowApiResponse.success(graphManager.create(instanceName, graphView));
    }

    @PutMapping("/instances/{instanceName}/graphs/{graphName}")
    public GeaflowApiResponse<Boolean> update(@PathVariable("instanceName") String instanceName,
                                              @PathVariable("graphName") String graphName,
                                              @RequestBody GraphView graphView) {
        return GeaflowApiResponse.success(graphManager.updateByName(instanceName, graphName, graphView));
    }


    @DeleteMapping("/instances/{instanceName}/graphs/{graphName}")
    public GeaflowApiResponse<Boolean> drop(@PathVariable("instanceName") String instanceName,
                                            @PathVariable("graphName") String graphName) {
        return GeaflowApiResponse.success(graphManager.dropByName(instanceName, graphName));
    }

    @PostMapping("/instances/{instanceName}/graphs/{graphName}/clean")
    public GeaflowApiResponse<Boolean> clean(@PathVariable("instanceName") String instanceName,
                                             @PathVariable("graphName") String graphName) {
        return GeaflowApiResponse.success(graphManager.clean(instanceName, graphName));
    }

    @PostMapping("/instances/{instanceName}/graphs/{graphName}/endpoints")
    public GeaflowApiResponse<Boolean> createEndpoints(@PathVariable("instanceName") String instanceName,
                                                       @PathVariable("graphName") String graphName,
                                                       @RequestBody List<EndpointView> endpoints) {
        return GeaflowApiResponse.success(graphManager.createEndpoints(instanceName, graphName, endpoints));
    }

    @DeleteMapping("/instances/{instanceName}/graphs/{graphName}/endpoints")
    public GeaflowApiResponse<Boolean> deleteEndpoints(@PathVariable("instanceName") String instanceName,
                                                       @PathVariable("graphName") String graphName,
                                                       @RequestBody(required = false) List<EndpointView> endpoints) {
        return GeaflowApiResponse.success(graphManager.deleteEndpoints(instanceName, graphName, endpoints));
    }

}
