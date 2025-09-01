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

import org.slf4j.Logger;

public class LoggerFormatter {

    public LoggerFormatter() {
    }

    public static String getCycleName(int cycleId) {
        return String.format("cycle#%s", cycleId);
    }

    public static String getCycleName(int cycleId, long windowId) {
        return String.format("cycle#%s-%s", cycleId, windowId);
    }

    public static String getCycleTag(String pipelineName, int cycleId) {
        return String.format("%s %s", pipelineName, getCycleName(cycleId));
    }

    public static String getCycleTag(String pipelineName, int cycleId, long windowId) {
        return String.format("%s %s", pipelineName, getCycleName(cycleId, windowId));
    }

    public static String getCycleMetricName(int cycleId, int vertexId) {
        return String.format("%s[%d]", getCycleName(cycleId), vertexId);
    }

    public static String getCycleMetricName(int cycleId, long windowId, int vertexId) {
        return String.format("%s[%d]", getCycleName(cycleId, windowId), vertexId);
    }

    public static String getTaskTag(String pipelineName, int cycleId,
                                    int taskId, int vertexId, int index, int parallelism) {
        return String.format("%s task#%d [%d-%d/%d]", getCycleTag(pipelineName, cycleId),
            taskId, vertexId, index, parallelism);
    }

    public static String getTaskTag(String pipelineName, int cycleId, long windowId,
                                    int taskId, int vertexId, int index, int parallelism) {
        return String.format("%s task#%d [%d-%d/%d]", getCycleTag(pipelineName, cycleId, windowId),
            taskId, vertexId, index, parallelism);
    }

    /**
     * Get the exception stack message in order to troubleshoot problems.
     *
     * @param e
     * @return
     */
    public static String getStackMsg(Exception e) {
        StringBuffer sb = new StringBuffer();
        StackTraceElement[] stackArray = e.getStackTrace();
        for (int i = 0; i < stackArray.length; i++) {
            StackTraceElement element = stackArray[i];
            sb.append(element.toString() + "\n");
        }
        return sb.toString();
    }

    public static void debug(Logger logger, String msg) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg);
        }
    }

    public static void debug(Logger logger, String msg, Object o) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg, o);
        }
    }

    public static void debug(Logger logger, String msg, Object... o) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg, o);
        }
    }

    public static void info(Logger logger, String msg) {
        if (logger.isInfoEnabled()) {
            logger.info(msg);
        }
    }

    public static void info(Logger logger, String msg, Object o) {
        if (logger.isInfoEnabled()) {
            logger.info(msg, o);
        }
    }

    public static void info(Logger logger, String msg, Object... o) {
        if (logger.isInfoEnabled()) {
            logger.info(msg, o);
        }
    }

    public static void info(Logger logger, String msg, Throwable t) {
        if (logger.isInfoEnabled()) {
            logger.info(msg, t);
        }
    }

    public static void warn(Logger logger, String msg) {
        logger.warn(msg);
    }

    public static void warn(Logger logger, String msg, Object... o) {
        logger.warn(msg, o);
    }

    public static void warn(Logger logger, String msg, Throwable t) {
        logger.warn(msg, t);
    }

    public static void error(Logger logger, String msg) {
        logger.error(msg);
    }

    public static void error(Logger logger, String msg, Object... o) {
        logger.error(msg, o);
    }

    public static void error(Logger logger, String msg, Throwable t) {
        logger.error(msg, t);
    }

}
