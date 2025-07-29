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

package org.apache.geaflow.cluster.collector;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.apache.geaflow.cluster.task.service.AbstractTaskService;
import org.apache.geaflow.common.config.Configuration;

public class EmitterService extends AbstractTaskService<IEmitterRequest, EmitterRunner> implements Serializable {

    private static final String EMITTER_FORMAT = "geaflow-emitter-%d";

    private final int slots;

    public EmitterService(int slots, Configuration configuration) {
        super(configuration, EMITTER_FORMAT);
        this.slots = slots;
    }

    protected EmitterRunner[] buildTaskRunner() {
        Preconditions.checkArgument(slots > 0, "fetcher pool should be larger than 0");
        EmitterRunner[] emitterRunners = new EmitterRunner[slots];
        for (int i = 0; i < slots; i++) {
            EmitterRunner runner = new EmitterRunner(this.configuration, i);
            emitterRunners[i] = runner;
        }
        return emitterRunners;
    }
}
