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

package org.apache.geaflow.env.args;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_APP_NAME;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentArgumentParser implements IEnvironmentArgsParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentArgumentParser.class);

    private static final String JOB_ARGS = "job";
    private static final String SYSTEM_ARGS = "system";
    private static final String CLUSTER_ARGS = "cluster";
    private static final String GEAFLOW_PREFIX = "geaflow";

    private static final String STATE_CONFIG = "stateConfig";
    private static final String METRIC_CONFIG = "metricConfig";

    @Override
    public Map<String, String> parse(String[] args) {
        if (args == null || args.length == 0) {
            return Collections.emptyMap();
        }

        LOGGER.warn("user config: {}", Arrays.asList(args));
        Map<String, String> mainArgs = JsonUtils.parseJson2map(StringEscapeUtils.unescapeJava(args[0]));

        Map<String, String> systemArgs = null;
        if (mainArgs.containsKey(SYSTEM_ARGS)) {
            systemArgs = parseSystemArgs(mainArgs.remove(SYSTEM_ARGS));
        }

        Map<String, String> userArgs = null;
        if (mainArgs.containsKey(JOB_ARGS)) {
            userArgs = JsonUtils.parseJson2map(mainArgs.remove(JOB_ARGS));
        }

        if (systemArgs != null && userArgs != null) {
            Set<String> systemArgsKeys = systemArgs.keySet();
            Set<String> userArgsKeys = userArgs.keySet();
            if (userArgsKeys.stream().anyMatch(systemArgsKeys::contains)) {
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.keyConflictsError(userArgsKeys, systemArgsKeys));
            }
        }

        Map<String, String> clusterArgs = null;
        if (mainArgs.containsKey(CLUSTER_ARGS)) {
            clusterArgs = JsonUtils.parseJson2map(mainArgs.remove(CLUSTER_ARGS));
        }

        LOGGER.info("build Env with systemArgs: {}, ", JsonUtils.toJsonString(systemArgs));
        LOGGER.info("build Env with userArgs: {}", JsonUtils.toJsonString(userArgs));
        LOGGER.info("build Env with clusterArgs: {}", JsonUtils.toJsonString(clusterArgs));

        Map<String, String> envConfig = new HashMap<>();
        if (systemArgs != null) {
            envConfig.putAll(systemArgs);
        }
        if (clusterArgs != null) {
            envConfig.putAll(clusterArgs);
        }
        if (userArgs != null) {
            envConfig.putAll(userArgs);
        }
        LOGGER.info("build envConfig: {}", JsonUtils.toJsonString(envConfig));

        return envConfig;
    }

    private Map<String, String> parseSystemArgs(String jsonStr) {
        Map<String, String> systemArgs = JsonUtils.parseJson2map(jsonStr);

        ensureConfigExist(systemArgs, JOB_APP_NAME);
        ensureConfigExist(systemArgs, JOB_UNIQUE_ID);

        Map<String, String> finalSystemArgs = new HashMap<>();
        fillSystemConfig(finalSystemArgs, systemArgs);

        if (systemArgs.containsKey(STATE_CONFIG)) {
            Map<String, String> stateConfig = JsonUtils.parseJson2map(systemArgs.remove(STATE_CONFIG));
            fillSystemConfig(finalSystemArgs, stateConfig);

        }
        if (systemArgs.containsKey(METRIC_CONFIG)) {
            Map<String, String> metricConfig = JsonUtils.parseJson2map(systemArgs.remove(METRIC_CONFIG));
            fillSystemConfig(finalSystemArgs, metricConfig);
        }

        return finalSystemArgs;
    }

    private static void ensureConfigExist(Map<String, String> config, ConfigKey configKey) {
        String key = configKey.getKey();
        if (!config.containsKey(key)
            || !StringUtils.isNotBlank(config.get(key))) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.configKeyNotFound(key));
        }
    }

    private static void fillSystemConfig(Map<String, String> finalSystemArgs, Map<String, String> tmp) {
        for (Map.Entry<String, String> entry : tmp.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(GEAFLOW_PREFIX)) {
                finalSystemArgs.put(key, value);
            } else {
                LOGGER.warn("ignore nonstandard system config: {} {}", key, value);
            }
        }
    }

}
