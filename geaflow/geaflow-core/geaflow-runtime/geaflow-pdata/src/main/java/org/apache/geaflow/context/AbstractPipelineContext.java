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

package org.apache.geaflow.context;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.geaflow.api.pdata.base.PAction;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.IViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPipelineContext implements IPipelineContext, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPipelineContext.class);

    protected final AtomicInteger idGenerator = new AtomicInteger(0);
    protected Configuration pipelineConfig;
    protected transient List<PAction> actions;
    protected transient Map<String, IViewDesc> viewDescMap;

    public AbstractPipelineContext(Configuration pipelineConfig) {
        this.pipelineConfig = pipelineConfig;
        this.actions = new ArrayList<>();
        this.viewDescMap = new HashMap<>();
    }

    public int generateId() {
        return idGenerator.incrementAndGet();
    }

    @Override
    public void addPAction(PAction action) {
        LOGGER.info("Add Action, Id:{}", action.getId());
        this.actions.add(action);
    }

    public void addView(IViewDesc viewDesc) {
        LOGGER.info("User ViewName:{} ViewDesc:{}", viewDesc.getName(), viewDesc);
        this.viewDescMap.put(viewDesc.getName(), viewDesc);
    }

    public IViewDesc getViewDesc(String name) {
        IViewDesc viewDesc = this.viewDescMap.get(name);
        Preconditions.checkArgument(viewDesc != null);
        return viewDesc;
    }

    public Configuration getConfig() {
        return pipelineConfig;
    }

    public List<PAction> getActions() {
        return actions.stream().collect(Collectors.toList());
    }

    public Map<String, IViewDesc> getViewDescMap() {
        return viewDescMap;
    }

}
