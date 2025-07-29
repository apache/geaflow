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

package org.apache.geaflow.console.core.service.config;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.type.GeaflowDeployMode;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Getter
@Component
public class DeployConfig implements InitializingBean {

    @Value("${geaflow.host}")
    protected String host;

    @Value("${geaflow.gateway.port}")
    protected Integer gatewayPort;

    @Value("${geaflow.gateway.url}")
    protected String gatewayUrl;

    @Value("${geaflow.deploy.mode}")
    private GeaflowDeployMode mode = GeaflowDeployMode.LOCAL;

    public boolean isLocalMode() {
        return GeaflowDeployMode.LOCAL.equals(mode);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.gatewayUrl = StringUtils.removeEnd(gatewayUrl, "/");
    }
}
