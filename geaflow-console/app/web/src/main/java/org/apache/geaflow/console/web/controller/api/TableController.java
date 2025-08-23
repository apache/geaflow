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

import org.apache.geaflow.console.biz.shared.TableManager;
import org.apache.geaflow.console.biz.shared.view.TableView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.TableSearch;
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
public class TableController {

    @Autowired
    private TableManager tableManager;

    @GetMapping("/tables")
    public GeaflowApiResponse<PageList<TableView>> search(TableSearch search) {
        return GeaflowApiResponse.success(tableManager.search(search));
    }

    @GetMapping("/instances/{instanceName}/tables")
    public GeaflowApiResponse<PageList<TableView>> instanceSearch(@PathVariable("instanceName") String instanceName,
                                                                  TableSearch search) {
        return GeaflowApiResponse.success(tableManager.searchByInstanceName(instanceName, search));
    }

    @GetMapping("/instances/{instanceName}/tables/{tableName}")
    public GeaflowApiResponse<TableView> getTable(@PathVariable("instanceName") String instanceName,
                                                  @PathVariable("tableName") String tableName) {
        return GeaflowApiResponse.success(tableManager.getByName(instanceName, tableName));
    }

    @PostMapping("/instances/{instanceName}/tables")
    public GeaflowApiResponse<String> createTable(@PathVariable("instanceName") String instanceName,
                                                  @RequestBody TableView tableView) {
        return GeaflowApiResponse.success(tableManager.create(instanceName, tableView));
    }

    @PutMapping("/instances/{instanceName}/tables/{tableName}")
    public GeaflowApiResponse<Boolean> updateTable(@PathVariable("instanceName") String instanceName,
                                                   @PathVariable("tableName") String tableName,
                                                   @RequestBody TableView tableView) {
        return GeaflowApiResponse.success(tableManager.updateByName(instanceName, tableName, tableView));
    }


    @DeleteMapping("/instances/{instanceName}/tables/{tableName}")
    public GeaflowApiResponse<Boolean> dropTable(@PathVariable("instanceName") String instanceName,
                                                 @PathVariable("tableName") String tableName) {
        return GeaflowApiResponse.success(tableManager.dropByName(instanceName, tableName));
    }

}
