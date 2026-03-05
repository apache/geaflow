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
import org.apache.geaflow.console.common.util.exception.GeaflowExceptionClassifier;
import org.apache.geaflow.console.common.util.exception.GeaflowExceptionClassifier.GeaflowExceptionClassificationResult;

@Getter
public class ErrorApiResponse<T> extends GeaflowApiResponse<T> {

    private static final GeaflowExceptionClassifier EXCEPTION_CLASSIFIER = new GeaflowExceptionClassifier();

    private final GeaflowApiRequest<?> request;

    private final String message;

    protected ErrorApiResponse(Throwable error) {
        super(false);

        // Use classifier to classify the exception
        GeaflowExceptionClassificationResult result = EXCEPTION_CLASSIFIER.classify(error);
        this.code = result.getCode();
        this.message = result.getMessage();

        this.request = GeaflowApiRequest.currentRequest();
    }

    @Override
    public void write(HttpServletResponse response) {
        response.reset();
        // Use centralized CORS configuration
        ErrorApiCorsConfigurer.configure(response);
        super.write(response);
    }
}
