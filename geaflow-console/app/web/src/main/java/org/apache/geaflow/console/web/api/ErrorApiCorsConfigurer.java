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

import javax.servlet.http.HttpServletResponse;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.springframework.http.HttpHeaders;

public class ErrorApiCorsConfigurer {

    private ErrorApiCorsConfigurer() {
    }

    public static void configure(HttpServletResponse response) {
        String originKey = HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
        String credentialsKey = HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS;
        String headersKey = HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS;
        String methodsKey = HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS;
        String ageKey = HttpHeaders.ACCESS_CONTROL_MAX_AGE;

        response.setHeader(originKey, ContextHolder.get().getRequest().getHeader(HttpHeaders.ORIGIN));
        response.setHeader(methodsKey, "OPTIONS,HEAD,GET,POST,PUT,PATCH,DELETE,TRACE");
        response.setHeader(headersKey, "Origin,X-Requested-With,Content-Type,Accept,geaflow-token,geaflow-task-token");
        response.setHeader(credentialsKey, "true");
        response.setHeader(ageKey, "3600");
    }
}
