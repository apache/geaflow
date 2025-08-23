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

import org.apache.geaflow.console.biz.shared.PluginManager;
import org.apache.geaflow.console.biz.shared.view.PluginView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.PluginSearch;
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
@RequestMapping("/plugins")
public class PluginController {

    @Autowired
    private PluginManager pluginManager;

    @GetMapping("/{pluginId}")
    public GeaflowApiResponse<PluginView> queryPlugin(@PathVariable String pluginId) {
        return GeaflowApiResponse.success(pluginManager.get(pluginId));
    }

    @GetMapping
    public GeaflowApiResponse<PageList<PluginView>> searchPlugins(PluginSearch search) {
        return GeaflowApiResponse.success(pluginManager.search(search));
    }

    @PostMapping
    public GeaflowApiResponse<String> createPlugin(PluginView pluginView,
                                                   @RequestParam(required = false) MultipartFile jarFile,
                                                   @RequestParam(required = false) String fileId) {
        return GeaflowApiResponse.success(pluginManager.createPlugin(pluginView, jarFile, fileId));
    }

    @PutMapping("/{pluginId}")
    public GeaflowApiResponse<Boolean> updatePlugin(@PathVariable String pluginId, PluginView view,
                                                    @RequestParam(required = false) MultipartFile jarFile) {
        return GeaflowApiResponse.success(pluginManager.updatePlugin(pluginId, view, jarFile));
    }

    @DeleteMapping("/{pluginId}")
    public GeaflowApiResponse<Boolean> deletePlugin(@PathVariable String pluginId) {
        return GeaflowApiResponse.success(pluginManager.drop(pluginId));
    }


}
