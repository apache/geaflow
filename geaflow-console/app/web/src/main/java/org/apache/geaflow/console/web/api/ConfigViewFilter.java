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

package org.apache.geaflow.console.web.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import org.apache.geaflow.console.biz.shared.view.ClusterView;
import org.apache.geaflow.console.biz.shared.view.LLMView;
import org.apache.geaflow.console.biz.shared.view.PluginConfigView;
import org.apache.geaflow.console.biz.shared.view.ReleaseView;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.ConfigDescFactory;
import org.apache.geaflow.console.core.model.config.ConfigRedactor;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.job.config.ClusterConfigClass;

/** Applies equally to detail, page, and nested table/graph responses, including administrator responses. */
public class ConfigViewFilter implements ValueFilter {

    @Override
    public Object process(Object object, String name, Object value) {
        if (object instanceof ClusterView && "config".equals(name)) {
            return ConfigRedactor.redact((GeaflowConfig) value, ConfigRedactor.pluginDesc(((ClusterView) object).getType()));
        }
        if (object instanceof PluginConfigView && "config".equals(name)) {
            return ConfigRedactor.redact((GeaflowConfig) value,
                ConfigRedactor.pluginDesc(GeaflowPluginType.of(((PluginConfigView) object).getType())));
        }
        if (object instanceof LLMView && "args".equals(name) && value != null) {
            return JSON.toJSONString(ConfigRedactor.redact(JSON.parseObject((String) value, GeaflowConfig.class),
                ConfigRedactor.llmDesc(((LLMView) object).getType())));
        }
        if (object instanceof ReleaseView && "jobConfig".equals(name)) {
            ReleaseView release = (ReleaseView) object;
            return ConfigRedactor.redact((GeaflowConfig) value,
                ConfigRedactor.jobDesc(release.getJob() == null ? null : release.getJob().getType()));
        }
        if (object instanceof ReleaseView && "clusterConfig".equals(name)) {
            return ConfigRedactor.redact((GeaflowConfig) value, ConfigDescFactory.getOrRegister(ClusterConfigClass.class));
        }
        return value;
    }

}
