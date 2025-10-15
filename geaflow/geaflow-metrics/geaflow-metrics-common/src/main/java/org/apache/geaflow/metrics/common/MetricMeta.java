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

package org.apache.geaflow.metrics.common;

import java.io.Serializable;

public class MetricMeta implements Serializable {

    private String jobName;
    private String metricName;
    private String metricType;
    private String queries;
    private String metricGroup;
    private String metaType;
    private String metaGroup;

    public String getJobName() {
        return this.jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getMetricName() {
        return this.metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getMetricType() {
        return this.metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public String getQueries() {
        return this.queries;
    }

    public void setQueries(String queries) {
        this.queries = queries;
    }

    public String getMetricGroup() {
        return this.metricGroup;
    }

    public void setMetricGroup(String metricGroup) {
        this.metricGroup = metricGroup;
    }

    public String getMetaType() {
        return this.metaType;
    }

    public void setMetaType(String metaType) {
        this.metaType = metaType;
    }

    public String getMetaGroup() {
        return this.metaGroup;
    }

    public void setMetaGroup(String metaGroup) {
        this.metaGroup = metaGroup;
    }
}
