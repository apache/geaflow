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

package org.apache.geaflow.ai.retrieval.channel.graph;

import java.util.Objects;

/** Typed failure raised while validating or opening a logical graph view. */
public final class GraphViewException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    /** Stable graph view failure categories. */
    public enum Code {
        MISSING_PARENT,
        DELTA_CONFLICT,
        BAD_REFERENCE,
        NOT_READY,
        VERSION_CONFLICT,
        NOT_FOUND,
        INVALID_VIEW
    }

    private final Code code;

    public GraphViewException(Code code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public GraphViewException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    public Code getCode() {
        return code;
    }
}
