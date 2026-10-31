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

package org.apache.geaflow.dsl.connector.clickhouse;

import java.util.function.BooleanSupplier;
import org.testng.SkipException;

final class ClickHouseTestEnvironment {

    private static final String MESSAGE = "ClickHouse integration tests need a reachable Docker daemon. "
        + "Check docker version, docker info, socket permissions and DOCKER_HOST. "
        + "CI requires these tests; use a supported Docker runtime and Testcontainers client.";

    private ClickHouseTestEnvironment() {
    }

    static boolean isDockerRequired(String configured, String ci) {
        if ("true".equalsIgnoreCase(configured)) {
            return true;
        }
        if ("false".equalsIgnoreCase(configured)) {
            return false;
        }
        if (configured != null) {
            throw new IllegalArgumentException("clickhouse.it.required must be true or false");
        }
        return ci != null && !ci.isEmpty() && !"false".equalsIgnoreCase(ci) && !"0".equals(ci);
    }

    static void requireDocker(boolean required, BooleanSupplier dockerAvailable) {
        boolean available;
        try {
            available = dockerAvailable.getAsBoolean();
        } catch (RuntimeException error) {
            if (required) {
                throw new IllegalStateException(MESSAGE, error);
            }
            throw new SkipException(MESSAGE, error);
        }
        if (!available) {
            if (required) {
                throw new IllegalStateException(MESSAGE);
            }
            throw new SkipException(MESSAGE + " Skipping optional local integration tests.");
        }
    }
}
