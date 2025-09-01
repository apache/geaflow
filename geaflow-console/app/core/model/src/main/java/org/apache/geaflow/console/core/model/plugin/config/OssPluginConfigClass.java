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

package org.apache.geaflow.console.core.model.plugin.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.util.NetworkUtil;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;

@Getter
@Setter
public class OssPluginConfigClass extends PersistentPluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.file.oss.endpoint", comment = "i18n.key.endpoint")
    @GeaflowConfigValue(required = true, defaultValue = "cn-hangzhou.alipay.aliyun-inc.com")
    private String endpoint;

    @GeaflowConfigKey(value = "geaflow.file.oss.access.id", comment = "i18n.key.access.id")
    @GeaflowConfigValue(required = true)
    private String accessId;

    @GeaflowConfigKey(value = "geaflow.file.oss.secret.key", comment = "i18n.key.secret.key")
    @GeaflowConfigValue(required = true, masked = true)
    private String secretKey;

    @GeaflowConfigKey(value = "geaflow.file.oss.bucket.name", comment = "i18n.key.bucket")
    @GeaflowConfigValue(required = true)
    private String bucket;

    public OssPluginConfigClass() {
        super(GeaflowPluginType.OSS);
    }

    @Override
    public void testConnection() {
        if (NetworkUtil.getPort(endpoint) == null) {
            String host = NetworkUtil.getHost(endpoint);
            NetworkUtil.testHostPort(host, 80);

        } else {
            NetworkUtil.testUrl(endpoint);
        }
    }
}
