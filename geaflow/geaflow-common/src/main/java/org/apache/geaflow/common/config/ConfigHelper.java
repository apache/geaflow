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

package org.apache.geaflow.common.config;

import java.util.Map;
import org.apache.geaflow.common.exception.ConfigException;

public class ConfigHelper {

    public static int getInteger(Map config, String configKey) {
        if (config.containsKey(configKey)) {
            return Integer.valueOf(String.valueOf(config.get(configKey)));
        } else {
            throw new ConfigException(configKey);
        }
    }

    public static int getIntegerOrDefault(Map config, String configKey, int defaultValue) {
        if (config.containsKey(configKey)) {
            return Integer.valueOf(String.valueOf(config.get(configKey)));
        } else {
            return defaultValue;
        }
    }

    public static long getLong(Map config, String configKey) {
        if (config.containsKey(configKey)) {
            return Long.valueOf(String.valueOf(config.get(configKey)));
        } else {
            throw new ConfigException(configKey);
        }
    }

    public static long getLongOrDefault(Map config, String configKey, long defaultValue) {
        if (config.containsKey(configKey)) {
            return Long.valueOf(String.valueOf(config.get(configKey)));
        } else {
            return defaultValue;
        }
    }

    public static boolean getBoolean(Map config, String configKey) {
        if (config.containsKey(configKey)) {
            return Boolean.valueOf(String.valueOf(config.get(configKey)));
        } else {
            throw new ConfigException(configKey);
        }
    }

    public static boolean getBooleanOrDefault(Map config, String configKey, boolean defaultValue) {
        if (config.containsKey(configKey)) {
            return Boolean.valueOf(String.valueOf(config.get(configKey)));
        } else {
            return defaultValue;
        }
    }

    public static String getString(Map config, String configKey) {
        if (config.containsKey(configKey)) {
            return String.valueOf(config.get(configKey));
        } else {
            throw new ConfigException("Missing config:'" + configKey + "'");
        }
    }

    public static String getStringOrDefault(Map config, String configKey, String defaultValue) {
        if (config.containsKey(configKey)) {
            return String.valueOf(config.get(configKey));
        } else {
            return defaultValue;
        }
    }

    public static Double getDoubleOrDefault(Map config, String configKey, double defaultValue) {
        if (config.containsKey(configKey)) {
            return Double.valueOf(String.valueOf(config.get(configKey)));
        } else {
            return defaultValue;
        }
    }

    public static Double getDouble(Map config, String configKey) {
        if (config.containsKey(configKey)) {
            return Double.valueOf(String.valueOf(config.get(configKey)));
        } else {
            throw new ConfigException(configKey);
        }
    }

}
