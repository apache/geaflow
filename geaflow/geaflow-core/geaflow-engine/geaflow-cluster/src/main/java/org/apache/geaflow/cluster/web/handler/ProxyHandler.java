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

package org.apache.geaflow.cluster.web.handler;

import java.util.Arrays;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyHandler extends ProxyServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyHandler.class);

    @Override
    protected String rewriteTarget(HttpServletRequest request) {
        return getTargetUrl(request);
    }

    private String getTargetUrl(HttpServletRequest request) {
        String path = request.getRequestURI();
        String[] pathParts = path.split("/");
        String fullUri = String.join("/", Arrays.copyOfRange(pathParts, 2, pathParts.length));
        StringBuilder target = new StringBuilder();
        target.append("http://").append(fullUri);
        if (request.getQueryString() != null) {
            target.append("?").append(request.getQueryString());
        }
        return target.toString();
    }
}
