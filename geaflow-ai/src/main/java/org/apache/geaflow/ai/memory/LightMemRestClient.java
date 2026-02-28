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

package org.apache.geaflow.ai.memory;

import java.util.Objects;
import org.apache.geaflow.ai.protocol.AiEnvelope;
import org.apache.geaflow.ai.protocol.AiHttpClient;
import org.apache.geaflow.ai.protocol.AiScope;
import org.apache.geaflow.ai.protocol.AiTrace;

public class LightMemRestClient {

    private final AiHttpClient writeClient;
    private final AiHttpClient recallClient;

    public LightMemRestClient(LightMemConfig config) {
        Objects.requireNonNull(config);
        this.writeClient = new AiHttpClient(config.writeUrl(), config.token);
        this.recallClient = new AiHttpClient(config.recallUrl(), config.token);
    }

    public MemoryWriteResponse write(AiScope scope, AiTrace trace, MemoryWriteRequest request) {
        AiEnvelope env = AiEnvelope.of(scope, trace, request);
        return writeClient.executePayload(env, MemoryWriteResponse.class);
    }

    public MemoryRecallResponse recall(AiScope scope, AiTrace trace, MemoryRecallRequest request) {
        AiEnvelope env = AiEnvelope.of(scope, trace, request);
        return recallClient.executePayload(env, MemoryRecallResponse.class);
    }
}

