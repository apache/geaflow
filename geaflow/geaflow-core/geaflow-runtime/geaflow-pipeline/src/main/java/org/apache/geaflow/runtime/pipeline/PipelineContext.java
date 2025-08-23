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

package org.apache.geaflow.runtime.pipeline;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.context.AbstractPipelineContext;

public class PipelineContext extends AbstractPipelineContext {

    private transient AtomicInteger idGenerator;
    private String name;

    public PipelineContext(String name, Configuration pipelineConfig) {
        super(pipelineConfig);
        this.name = name;
        this.actions = new ArrayList<>();
        this.idGenerator = new AtomicInteger(0);
    }

    public int generateId() {
        return idGenerator.addAndGet(1);
    }

    public String getName() {
        return name;
    }

}
