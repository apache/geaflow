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

package org.apache.geaflow.ai.retrieval.metadata;

import java.util.Locale;

/** Validation shared by metadata constructors and their JSON boundary. */
final class MetadataValidation {

    private MetadataValidation() {
    }

    static String required(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw invalid(field + " is required");
        }
        return value;
    }

    static String checksum(String value) {
        if (value == null || !value.matches("[a-fA-F0-9]{64}")) {
            throw invalid("sha256 must contain 64 hexadecimal characters");
        }
        return value.toLowerCase(Locale.ROOT);
    }

    static long nonNegative(long value, String field) {
        if (value < 0) {
            throw invalid(field + " must be non-negative");
        }
        return value;
    }

    static MetadataException invalid(String message) {
        return new MetadataException(MetadataException.Code.INVALID_METADATA, message);
    }
}
