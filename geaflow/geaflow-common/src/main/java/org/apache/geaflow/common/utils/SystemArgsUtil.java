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

/*
 * This Class is a modification Class from Netty.
 */

package org.apache.geaflow.common.utils;

import static java.util.Objects.requireNonNull;

import java.security.AccessController;
import java.security.PrivilegedAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemArgsUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemArgsUtil.class);

    private SystemArgsUtil() {
        // Unused
    }

    /**
     * Returns the value of the Java system property with the specified
     * {@code key}, while falling back to {@code null} if the property access fails.
     *
     * @return the property value or {@code null}
     */
    public static String get(String key) {
        return get(key, null);
    }

    /**
     * Returns the value of the Java system property with the specified
     * {@code key}, while falling back to the specified default value if
     * the property access fails.
     *
     * @return the property value.
     *     {@code def} if there's no such property or if an access to the
     *     specified property is not allowed.
     */
    public static String get(final String key, String def) {
        requireNonNull(key, "key");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty.");
        }

        String value = null;
        try {
            if (System.getSecurityManager() == null) {
                value = System.getProperty(key);
            } else {
                value = AccessController
                    .doPrivileged((PrivilegedAction<String>) () -> System.getProperty(key));
            }
        } catch (Exception ignore) {
            LOGGER.warn("Unable to retrieve a system property '{}'; default values will be used.",
                key, ignore);
        }

        if (value == null) {
            return def;
        }

        return value;
    }

}
