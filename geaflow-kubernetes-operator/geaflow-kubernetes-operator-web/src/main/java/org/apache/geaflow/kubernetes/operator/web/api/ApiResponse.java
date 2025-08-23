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

package org.apache.geaflow.kubernetes.operator.web.api;

import com.alibaba.fastjson.JSON;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import lombok.Getter;
import org.apache.geaflow.kubernetes.operator.core.model.exception.GeaflowRuntimeException;
import org.apache.geaflow.kubernetes.operator.core.util.CommonUtil;
import org.springframework.http.MediaType;

@Getter
public class ApiResponse<T> {

    private static final int HTTP_SUCCESS_CODE = 200;
    private final boolean success;
    private final T data;
    private final String host;
    private final String message;

    protected ApiResponse(boolean success, T data, String message) {
        this.success = success;
        this.message = message;
        this.data = data;
        this.host = CommonUtil.getHostName();
    }

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse(true, data, null);
    }

    public static <T> ApiResponse<T> error(Throwable error) {
        return new ApiResponse(false, null, error.getMessage());
    }

    public void write(HttpServletResponse response) {
        try {
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding(StandardCharsets.UTF_8.toString());
            response.setStatus(HTTP_SUCCESS_CODE);
            PrintWriter out = response.getWriter();
            JSON.writeJSONString(out, this);
            out.flush();
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Write api response failed", e);
        }
    }
}