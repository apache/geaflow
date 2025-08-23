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

package org.apache.geaflow.console.core.service.factory;

import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.data.GeaflowData;
import org.apache.geaflow.console.core.model.data.GeaflowEdge;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowTable;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.springframework.stereotype.Component;

@Component
public class GeaflowDataFactory {

    /**
     * generate resource data by resource type.
     */
    public static GeaflowData get(String name, String comment, String instanceId, GeaflowResourceType resourceType) {
        GeaflowData data;
        switch (resourceType) {
            case GRAPH:
                data = new GeaflowGraph(name, comment);
                ((GeaflowGraph) data).setPluginConfig(new GeaflowPluginConfig());
                break;
            case TABLE:
                data = new GeaflowTable(name, comment);
                ((GeaflowTable) data).setPluginConfig(new GeaflowPluginConfig());
                break;
            case VERTEX:
                data = new GeaflowVertex(name, comment);
                break;
            case EDGE:
                data = new GeaflowEdge(name, comment);
                break;
            case FUNCTION:
                data = new GeaflowFunction(name, comment);
                break;
            default:
                throw new GeaflowException("Unsupported resource type", resourceType);
        }
        data.setInstanceId(instanceId);
        return data;
    }
}
