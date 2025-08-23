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

package org.apache.geaflow.console.biz.shared.impl;

import java.util.Arrays;
import java.util.List;
import org.apache.geaflow.console.biz.shared.LLMManager;
import org.apache.geaflow.console.biz.shared.convert.LLMViewConverter;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.view.LLMView;
import org.apache.geaflow.console.common.dal.model.LLMSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowLLMType;
import org.apache.geaflow.console.core.model.llm.GeaflowLLM;
import org.apache.geaflow.console.core.service.LLMService;
import org.apache.geaflow.console.core.service.NameService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LLMManagerImpl extends NameManagerImpl<GeaflowLLM, LLMView, LLMSearch> implements
    LLMManager {

    @Autowired
    private LLMService llmService;

    @Autowired
    private LLMViewConverter llmViewConverter;

    @Override
    protected NameService<GeaflowLLM, ?, LLMSearch> getService() {
        return llmService;
    }

    @Override
    protected NameViewConverter<GeaflowLLM, LLMView> getConverter() {
        return llmViewConverter;
    }

    @Override
    protected List<GeaflowLLM> parse(List<LLMView> views) {
        return ListUtil.convert(views, llmViewConverter::convert);
    }

    @Override
    public List<GeaflowLLMType> getLLMTypes() {
        return Arrays.asList(GeaflowLLMType.values());
    }
}
