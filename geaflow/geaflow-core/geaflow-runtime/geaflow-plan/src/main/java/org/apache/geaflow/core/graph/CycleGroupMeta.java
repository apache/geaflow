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

package org.apache.geaflow.core.graph;

import org.apache.geaflow.plan.graph.AffinityLevel;

/**
 * The CycleGroupMeta which is used to describe group meta(e.g loop count, whether is iterative) of cycle.
 */
public class CycleGroupMeta {

    private long iterationCount;
    private int flyingCount;
    private CycleGroupType groupType;
    private AffinityLevel affinityLevel;

    public CycleGroupMeta() {
        this.iterationCount = 1;
        this.flyingCount = 1;
        this.groupType = CycleGroupType.pipelined;
        this.affinityLevel = AffinityLevel.worker;
    }

    public long getIterationCount() {
        return iterationCount;
    }

    public void setIterationCount(long iterationCount) {
        this.iterationCount = iterationCount;
    }

    public int getFlyingCount() {
        return flyingCount;
    }

    public void setFlyingCount(int flyingCount) {
        this.flyingCount = flyingCount;
    }

    public boolean isIterative() {
        return groupType == CycleGroupType.incremental || groupType == CycleGroupType.statical;
    }

    public CycleGroupType getGroupType() {
        return groupType;
    }

    public void setGroupType(CycleGroupType groupType) {
        this.groupType = groupType;
    }

    public AffinityLevel getAffinityLevel() {
        return affinityLevel;
    }

    public void setAffinityLevel(AffinityLevel affinityLevel) {
        this.affinityLevel = affinityLevel;
    }

    @Override
    public String toString() {
        return "CycleGroupMeta{"
            + "iterationCount=" + iterationCount
            + ", flyingCount=" + flyingCount
            + ", groupType=" + groupType
            + '}';
    }
}
