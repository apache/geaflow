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

package org.apache.geaflow.common.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShellUtil.class);

    public static void executeShellCommand(ProcessBuilder command, int timeoutSeconds) {
        Process process = null;
        try {
            LOGGER.info("Start executing shell command: {}", command.command());
            process = command.start();
            boolean exit = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!exit) {
                throw new GeaflowRuntimeException(
                    String.format("Command %s execute timeout.", command.command()));
            }
            int code = process.exitValue();
            if (code != 0) {
                String message = getCommandErrorMessage(process);
                LOGGER.error("Execute command {} failed with code {}. Error message: {}",
                    command, code, message);
                throw new GeaflowRuntimeException(
                    String.format("Code: %s, Message: %s", code, message));
            }
            LOGGER.info("Finished executing shell command finished: {}", command.command());
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        } finally {
            if (process != null && process.isAlive()) {
                LOGGER.info("Killed subprocess generated by command {}.", command.command());
                process.destroy();
            }
        }
    }

    public static String getCommandErrorMessage(Process process) {
        String errorMessage;
        try (InputStream inputStream = process.getErrorStream()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
            }
            errorMessage = stringBuilder.toString();
            if (StringUtils.isNotEmpty(errorMessage) && errorMessage.endsWith("\n")) {
                errorMessage = errorMessage.substring(0, errorMessage.length() - 1);
            }
        } catch (Exception e) {
            errorMessage = "Get error message from error-stream of process failed.";
            LOGGER.warn(errorMessage);
        }
        return errorMessage;
    }

}
