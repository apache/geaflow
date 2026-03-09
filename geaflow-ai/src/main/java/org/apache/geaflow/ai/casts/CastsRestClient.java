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

package org.apache.geaflow.ai.casts;

import java.util.Objects;
import org.apache.geaflow.ai.protocol.AiEnvelope;
import org.apache.geaflow.ai.protocol.AiHttpClient;
import org.apache.geaflow.ai.protocol.AiScope;
import org.apache.geaflow.ai.protocol.AiTrace;

public class CastsRestClient {

    private final AiHttpClient client;

    public CastsRestClient(CastsConfig config) {
        Objects.requireNonNull(config);
        this.client = new AiHttpClient(config.decisionUrl(), config.token);
    }

    public CastsDecisionResponse decision(AiScope scope, AiTrace trace, CastsDecisionRequest payload) {
        AiEnvelope env = AiEnvelope.of(scope, trace, payload);
        return client.executePayload(env, CastsDecisionResponse.class);
    }
}

