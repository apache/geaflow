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

package org.apache.geaflow.store.rocksdb;

import org.apache.geaflow.common.exception.GeaflowRuntimeException;

// Partition type for rocksdb graph store
public enum PartitionType {
    LABEL(false, true),
    // TODO: Support dt partition
    DT(true, false),
    // TODO: Support label dt partition
    DT_LABEL(true, true),
    NONE(false, false);

    private final boolean dtPartition;
    private final boolean labelPartition;

    private static final PartitionType[] VALUES = values();

    public static PartitionType getEnum(String value) {
        for (PartitionType v : VALUES) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }
        throw new GeaflowRuntimeException("Illegal partition type " + value);
    }

    PartitionType(boolean dtPartition, boolean labelPartition) {
        this.dtPartition = dtPartition;
        this.labelPartition = labelPartition;
    }

    public boolean isDtPartition() {
        return dtPartition;
    }

    public boolean isLabelPartition() {
        return labelPartition;
    }

    public boolean isPartition() {
        return dtPartition || labelPartition;
    }
}