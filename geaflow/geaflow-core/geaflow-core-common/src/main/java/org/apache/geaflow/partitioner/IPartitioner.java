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

package org.apache.geaflow.partitioner;

import java.io.Serializable;
import org.apache.geaflow.api.partition.IPartition;

public interface IPartitioner<T> extends Serializable {

    /**
     * Returns op id.
     */
    int getOpId();

    /**
     * Returns the partition.
     */
    IPartition<T> getPartition();

    /**
     * Returns the partition type.
     */
    PartitionType getPartitionType();

    enum PartitionType {

        /**
         * Random partition.
         */
        forward(true),
        /**
         * Broadcast partition.
         */
        broadcast(true),
        /**
         * Key partition.
         */
        key(true),
        /**
         * Custom partition.
         */
        custom(false),
        /**
         * Iterator partition.
         */
        iterator(false);

        boolean enablePushUp;

        PartitionType(boolean enablePushUp) {
            this.enablePushUp = enablePushUp;
        }

        public boolean isEnablePushUp() {
            return enablePushUp;
        }
    }

}
