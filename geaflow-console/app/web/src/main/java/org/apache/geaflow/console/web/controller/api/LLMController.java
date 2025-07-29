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
import org.apache.geaflow.console.biz.shared.LLMManager;
import org.apache.geaflow.console.biz.shared.view.LLMView;
import org.apache.geaflow.console.common.dal.model.LLMSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.type.GeaflowLLMType;
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
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/llms")
public class LLMController {

    @Autowired
    private LLMManager llmManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping
    public GeaflowApiResponse<PageList<LLMView>> searchLLMs(LLMSearch search) {
        return GeaflowApiResponse.success(llmManager.search(search));
    }

    @GetMapping("/{llmName}")
    public GeaflowApiResponse<LLMView> queryLLM(@PathVariable String llmName) {
        return GeaflowApiResponse.success(llmManager.getByName(llmName));
    }

    @PostMapping
    public GeaflowApiResponse<String> createLLM(@RequestBody LLMView llmView) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(llmManager.create(llmView));
    }

    @PutMapping("/{llmName}")
    public GeaflowApiResponse<Boolean> updateLLM(@PathVariable String llmName,
                                                 @RequestBody LLMView llmView) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(llmManager.updateByName(llmName, llmView));
    }

    @DeleteMapping("/{llmName}")
    public GeaflowApiResponse<Boolean> deleteLLM(@PathVariable String llmName) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(llmManager.dropByName(llmName));
    }

    @GetMapping("/types")
    public GeaflowApiResponse<List<GeaflowLLMType>> getLLMTypes() {
        return GeaflowApiResponse.success(llmManager.getLLMTypes());
    }

}
