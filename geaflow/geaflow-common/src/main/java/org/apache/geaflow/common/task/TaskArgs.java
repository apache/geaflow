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

package org.apache.geaflow.common.task;

import java.io.Serializable;

/**
 * TaskArgs that denotes relevant information of task, including: taskId, taskIndex, taskParallelism and
 * maxParallelism, processIndex.
 */
public class TaskArgs implements Serializable {

    private int taskId;
    private int taskIndex;
    private String taskName;
    private int parallelism;
    private int maxParallelism;
    private int processIndex;

    public TaskArgs(int taskId, int taskIndex, String taskName, int parallelism,
                    int maxParallelism, int processIndex) {
        this.taskId = taskId;
        this.taskIndex = taskIndex;
        this.taskName = taskName;
        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
        this.processIndex = processIndex;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public String getTaskName() {
        return taskName;
    }

    public int getProcessIndex() {
        return processIndex;
    }

}
