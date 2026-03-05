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

package org.apache.geaflow.console.common.util.exception;

import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.type.GeaflowApiResponseCode;

public class GeaflowExceptionClassifier {

    public GeaflowExceptionClassificationResult classify(Throwable error) {
        GeaflowApiResponseCode code;
        String message = error.getMessage();

        if (StringUtils.isBlank(message)) {
            message = error.getClass().getSimpleName();
        }

        if (error instanceof GeaflowSecurityException) {
            code = GeaflowApiResponseCode.FORBIDDEN;

        } else if (error instanceof GeaflowIllegalException) {
            code = GeaflowApiResponseCode.ILLEGAL;

        } else if (error instanceof GeaflowCompileException) {
            code = GeaflowApiResponseCode.ERROR;
            message = ((GeaflowCompileException) error).getDisplayMessage();

        } else if (error instanceof GeaflowException) {
            code = GeaflowApiResponseCode.ERROR;

        } else if (error instanceof IllegalArgumentException) {
            code = GeaflowApiResponseCode.ILLEGAL;

        } else if (error instanceof NullPointerException) {
            code = GeaflowApiResponseCode.ERROR;

        } else {
            code = GeaflowApiResponseCode.FAIL;
            // Traverse to root cause for better error message
            while (error.getCause() != null) {
                error = error.getCause();
            }
            message = error.getMessage();
            if (StringUtils.isBlank(message)) {
                message = error.getClass().getSimpleName();
            }
        }

        return new GeaflowExceptionClassificationResult(code, message);
    }

    public static class GeaflowExceptionClassificationResult {
        private final GeaflowApiResponseCode code;
        private final String message;

        public GeaflowExceptionClassificationResult(GeaflowApiResponseCode code, String message) {
            this.code = code;
            this.message = message;
        }

        public GeaflowApiResponseCode getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }
}
