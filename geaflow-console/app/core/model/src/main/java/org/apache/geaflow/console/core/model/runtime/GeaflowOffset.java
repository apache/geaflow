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

package org.apache.geaflow.console.core.model.runtime;

import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.core.model.GeaflowId;

@Getter
@Setter
public class GeaflowOffset extends GeaflowId {

    private long offset;
    private long writeTime;
    private OffsetType type;
    private String partitionName;
    private long diff;

    public void formatTime() {
        if (type == OffsetType.TIMESTAMP) {
            String offsetString = String.valueOf(offset);
            if (offsetString.length() == 10) {
                // transfer to millisecond
                offset = offset * 1000;
            }
            diff = writeTime - offset;
        }
    }

    public enum OffsetType {
        TIMESTAMP,
        NON_TIMESTAMP
    }
}
