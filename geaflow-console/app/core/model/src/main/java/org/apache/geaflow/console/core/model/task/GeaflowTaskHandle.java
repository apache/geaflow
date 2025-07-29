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

package org.apache.geaflow.console.core.model.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;

@Setter
@Getter
public abstract class GeaflowTaskHandle {

    public GeaflowTaskHandle(GeaflowPluginType clusterType, String appId) {
        this.clusterType = clusterType;
        this.appId = appId;
    }

    // cluster type
    protected GeaflowPluginType clusterType;

    // app id
    protected String appId;


    public static GeaflowTaskHandle parse(String text) {
        if (StringUtils.isEmpty(text)) {
            return null;
        }

        JSONObject json = JSON.parseObject(text);
        GeaflowPluginType clusterType = GeaflowPluginType.of(json.get("clusterType").toString());
        switch (clusterType) {
            case CONTAINER:
                return json.toJavaObject(ContainerTaskHandle.class);
            case K8S:
                return json.toJavaObject(K8sTaskHandle.class);
            case RAY:
                return json.toJavaObject(RayTaskHandle.class);
            default:
                throw new GeaflowException("Unsupported cluster type {}", clusterType);
        }
    }

}
