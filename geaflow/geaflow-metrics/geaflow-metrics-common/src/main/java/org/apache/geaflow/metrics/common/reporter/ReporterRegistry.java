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

package org.apache.geaflow.metrics.common.reporter;

import java.util.HashMap;
import java.util.Map;

public class ReporterRegistry {

    public static final String SLF4J_REPORTER = "slf4j";
    public static final String TSDB_REPORTER = "tsdb";
    public static final String INFLUXDB_REPORTER = "influxdb";
    private static final Map<String, String> NAME_CLASS_MAP = new HashMap<>();

    static {
        register(SLF4J_REPORTER, "org.apache.geaflow.metrics.slf4j.Slf4jReporter");
        register(TSDB_REPORTER, "org.apache.geaflow.metrics.tsdb.TsdbMetricReporter");
        register(INFLUXDB_REPORTER, "org.apache.geaflow.metrics.influxdb.InfluxdbReporter");
    }

    public static void register(String name, String className) {
        NAME_CLASS_MAP.put(name, className);
    }

    public static String getClassByName(String reporterName) {
        return NAME_CLASS_MAP.get(reporterName);
    }

}
