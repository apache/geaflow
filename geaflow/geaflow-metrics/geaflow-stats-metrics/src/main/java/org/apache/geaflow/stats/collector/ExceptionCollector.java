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

package org.apache.geaflow.stats.collector;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.stats.model.ExceptionInfo;
import org.apache.geaflow.stats.model.ExceptionLevel;
import org.apache.geaflow.stats.model.StatsMetricType;
import org.apache.geaflow.stats.sink.IStatsWriter;

public class ExceptionCollector extends BaseStatsCollector {

    ExceptionCollector(IStatsWriter statsWriter, Configuration configuration) {
        super(statsWriter, configuration);
    }

    public void reportException(Throwable e) {
        reportException(ExceptionLevel.ERROR, e);
    }

    public void reportException(ExceptionLevel severityLevel, Throwable e) {
        ExceptionInfo log = new ExceptionInfo(ProcessUtil.getHostname(),
            ProcessUtil.getHostIp(), ProcessUtil.getProcessId(),
            ExceptionUtils.getStackTrace(e), severityLevel.name());
        addToWriterQueue(genExceptionKey(), log);
    }

    private String genExceptionKey() {
        return jobName + StatsMetricType.Exception.getValue() + (Long.MAX_VALUE - System.currentTimeMillis());
    }

}
