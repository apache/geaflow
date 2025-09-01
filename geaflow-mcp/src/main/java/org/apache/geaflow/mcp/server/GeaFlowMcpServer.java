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

package org.apache.geaflow.mcp.server;

import org.noear.solon.Solon;
import org.noear.solon.ai.chat.tool.MethodToolProvider;
import org.noear.solon.ai.mcp.server.McpServerEndpointProvider;
import org.noear.solon.ai.mcp.server.resource.MethodResourceProvider;
import org.noear.solon.annotation.Controller;

/**
 * Main entrance, support jdk8+ environment.
 */
@Controller
public class GeaFlowMcpServer {
    private static final String SERVER_NAME = "geaflow-mcp-server";
    private static final String SSE_ENDPOINT = "/geaflow/sse";

    public static void main(String[] args) {
        Solon.start(GeaFlowMcpServer.class, args, app -> {
            // Manually build the mcp service endpoint.
            McpServerEndpointProvider endpointProvider = McpServerEndpointProvider.builder()
                .name(SERVER_NAME)
                .sseEndpoint(SSE_ENDPOINT)
                .build();
            endpointProvider.addTool(new MethodToolProvider(new GeaFlowMcpServerTools()));
            endpointProvider.addResource(new MethodResourceProvider(new GeaFlowMcpServerTools()));
            endpointProvider.postStart();
        });
    }
}
