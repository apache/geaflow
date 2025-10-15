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

package org.apache.geaflow.env;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_OFFSET_BACKEND_TYPE;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.apache.geaflow.env.args.EnvironmentArgumentParser;
import org.apache.geaflow.env.args.IEnvironmentArgsParser;
import org.apache.geaflow.state.StoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentFactory.class);

    public static Environment onLocalEnvironment() {
        Map<String, String> config = new HashMap<>();
        config.put(ExecutionConfigKeys.HTTP_REST_SERVICE_ENABLE.getKey(), Boolean.FALSE.toString());
        return onLocalEnvironment(config);
    }

    public static Environment onLocalEnvironment(String[] args) {
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        Map<String, String> config = new HashMap<>(argsParser.parse(args));
        return onLocalEnvironment(config);
    }

    private static Environment onLocalEnvironment(Map<String, String> config) {
        config.put(RUN_LOCAL_MODE.getKey(), Boolean.TRUE.toString());
        // Set default state backend type to memory on local env.
        config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        if (!config.containsKey(SYSTEM_OFFSET_BACKEND_TYPE.getKey())) {
            config.put(SYSTEM_OFFSET_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        }
        Environment environment = (Environment) loadEnvironment(EnvType.LOCAL);
        environment.getEnvironmentContext().withConfig(config);
        return environment;
    }

    public static Environment onRayEnvironment() {
        return (Environment) loadEnvironment(EnvType.RAY);
    }

    public static Environment onRayEnvironment(String[] args) {
        Environment environment = (Environment) loadEnvironment(EnvType.RAY);
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        environment.getEnvironmentContext().withConfig(argsParser.parse(args));
        return environment;
    }

    public static Environment onK8SEnvironment() {
        return (Environment) loadEnvironment(EnvType.K8S);
    }

    public static Environment onK8SEnvironment(String[] args) {
        Environment environment = (Environment) loadEnvironment(EnvType.K8S);
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        environment.getEnvironmentContext().withConfig(argsParser.parse(args));
        return environment;
    }

    private static IEnvironment loadEnvironment(EnvType envType) {
        ServiceLoader<IEnvironment> contextLoader = ServiceLoader.load(IEnvironment.class);
        Iterator<IEnvironment> contextIterable = contextLoader.iterator();
        while (contextIterable.hasNext()) {
            IEnvironment environment = contextIterable.next();
            if (environment.getEnvType() == envType) {
                LOGGER.info("loaded IEnvironment implementation {}", environment);
                return environment;
            }
        }
        LOGGER.error("NOT found IEnvironment implementation with type:{}", envType);
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(IEnvironment.class.getSimpleName()));
    }

    private static IEnvironmentArgsParser loadEnvironmentArgsParser() {
        ServiceLoader<IEnvironmentArgsParser> contextLoader = ServiceLoader.load(IEnvironmentArgsParser.class);
        Iterator<IEnvironmentArgsParser> contextIterable = contextLoader.iterator();
        boolean hasNext = contextIterable.hasNext();
        IEnvironmentArgsParser argsParser;
        if (hasNext) {
            argsParser = contextIterable.next();
        } else {
            // Use default argument parser.
            argsParser = new EnvironmentArgumentParser();
        }
        LOGGER.info("loaded IEnvironmentArgsParser implementation {}", argsParser);
        return argsParser;
    }
}
