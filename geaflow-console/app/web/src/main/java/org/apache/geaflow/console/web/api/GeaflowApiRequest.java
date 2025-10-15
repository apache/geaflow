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

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import lombok.Getter;
import lombok.Setter;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Getter
public class GeaflowApiRequest<T> {

    private static final String GEAFLOW_TOKEN_KEY = "geaflow-token";

    private String url;

    @Setter
    private RequestMethod method;

    @Setter
    private T body;

    public static GeaflowApiRequest<?> currentRequest() {
        GeaflowApiRequest<?> apiRequest = new GeaflowApiRequest<>();

        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();
            apiRequest.url = request.getRequestURI();
            apiRequest.method = RequestMethod.valueOf(request.getMethod());
        }
        return apiRequest;
    }

    public static String getSessionToken(HttpServletRequest request) {
        return getRequestParameter(request, GEAFLOW_TOKEN_KEY);
    }

    public static String getRequestParameter(HttpServletRequest request, String key) {
        String arg = getUrlParameter(request, key);
        if (arg == null) {
            arg = getHeader(request, key);
        }
        if (arg == null) {
            arg = getCookie(request, key);
        }
        return arg;
    }

    public static String getUrlParameter(HttpServletRequest request, String key) {
        return request.getParameter(key);
    }

    public static String getHeader(HttpServletRequest request, String key) {
        return request.getHeader(key);
    }

    public static String getCookie(HttpServletRequest request, String key) {
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (key.equals(cookie.getName())) {
                    return cookie.getValue();
                }
            }
        }
        return null;
    }
}
