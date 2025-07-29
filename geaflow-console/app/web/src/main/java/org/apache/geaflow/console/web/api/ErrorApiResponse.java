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
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowCompileException;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.exception.GeaflowSecurityException;
import org.apache.geaflow.console.common.util.type.GeaflowApiResponseCode;
import org.springframework.http.HttpHeaders;

@Getter
public class ErrorApiResponse<T> extends GeaflowApiResponse<T> {

    private final GeaflowApiRequest<?> request;

    private final String message;

    protected ErrorApiResponse(Throwable error) {
        super(false);

        String message = error.getMessage();
        if (StringUtils.isBlank(message)) {
            message = error.getClass().getSimpleName();
        }

        if (error instanceof GeaflowSecurityException) {
            this.code = GeaflowApiResponseCode.FORBIDDEN;

        } else if (error instanceof GeaflowIllegalException) {
            this.code = GeaflowApiResponseCode.ILLEGAL;

        } else if (error instanceof GeaflowCompileException) {
            this.code = GeaflowApiResponseCode.ERROR;
            message = ((GeaflowCompileException) error).getDisplayMessage();

        } else if (error instanceof GeaflowException) {
            this.code = GeaflowApiResponseCode.ERROR;

        } else if (error instanceof IllegalArgumentException) {
            this.code = GeaflowApiResponseCode.ILLEGAL;

        } else if (error instanceof NullPointerException) {
            this.code = GeaflowApiResponseCode.ERROR;

        } else {
            this.code = GeaflowApiResponseCode.FAIL;
            while (error.getCause() != null) {
                error = error.getCause();
            }
        }

        this.request = GeaflowApiRequest.currentRequest();
        this.message = message;
    }

    @Override
    public void write(HttpServletResponse response) {
        response.reset();
        configCors(response);
        super.write(response);
    }

    private void configCors(HttpServletResponse response) {
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
