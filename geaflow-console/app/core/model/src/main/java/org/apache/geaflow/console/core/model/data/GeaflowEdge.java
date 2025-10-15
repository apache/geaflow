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

package org.apache.geaflow.console.core.model.data;

import static org.apache.geaflow.console.common.util.type.GeaflowFieldCategory.EDGE_SOURCE_ID;
import static org.apache.geaflow.console.common.util.type.GeaflowFieldCategory.EDGE_TARGET_ID;
import static org.apache.geaflow.console.common.util.type.GeaflowFieldCategory.EDGE_TIMESTAMP;

import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.util.type.GeaflowFieldCategory;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;

@Getter
@Setter
public class GeaflowEdge extends GeaflowStruct {

    public GeaflowEdge() {
        super(GeaflowStructType.EDGE);
    }

    public GeaflowEdge(String name, String comment) {
        this();
        super.name = name;
        super.comment = comment;
    }

    @Override
    public void validate() {
        super.validate();

        int sourceIdCount = 0;
        int targetIdCount = 0;
        int tsCount = 0;
        for (GeaflowField value : fields.values()) {
            GeaflowFieldCategory category = value.getCategory();
            if (category == EDGE_SOURCE_ID) {
                sourceIdCount++;
            }

            if (category == EDGE_TARGET_ID) {
                targetIdCount++;
            }

            if (category == EDGE_TIMESTAMP) {
                tsCount++;
            }
        }

        EDGE_SOURCE_ID.validate(sourceIdCount);
        EDGE_TARGET_ID.validate(targetIdCount);
        EDGE_TIMESTAMP.validate(tsCount);
    }

}
