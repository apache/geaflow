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

package org.apache.geaflow.env.ctx;

import java.util.Map;
import java.util.UUID;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;

public class EnvironmentContext implements IEnvironmentContext {

    private final Configuration config;

    public EnvironmentContext() {
        this.config = new Configuration();
        buildDefaultConfig();
    }

    @Override
    public void withConfig(Map<String, String> config) {
        if (config != null) {
            this.config.putAll(config);
        }
    }

    public Configuration getConfig() {
        return config;
    }

    private void buildDefaultConfig() {
        String jobUid = UUID.randomUUID().toString();
        this.config.put(ExecutionConfigKeys.JOB_UNIQUE_ID, jobUid);
        this.config.put(ExecutionConfigKeys.JOB_APP_NAME, "geaflow" + jobUid);
    }

}
