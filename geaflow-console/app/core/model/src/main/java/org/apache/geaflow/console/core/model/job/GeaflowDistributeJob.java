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

package org.apache.geaflow.console.core.model.job;

import java.util.List;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;
import org.apache.geaflow.console.core.model.code.GeaflowCode;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowStruct;
import org.apache.geaflow.console.core.model.data.GeaflowTable;

public class GeaflowDistributeJob extends GeaflowTransferJob {


    public GeaflowDistributeJob() {
        super(GeaflowJobType.DISTRIBUTE);
    }

    public void fromGraphToTable(GeaflowGraph graph, GeaflowStructType type, String name, GeaflowTable table,
                                 List<FieldMappingItem> fieldMapping) {
        GeaflowStruct struct = super.importGraphStruct(graph, type, name);
        super.addStructMapping(struct, table, fieldMapping);
    }

    @Override
    public GeaflowCode generateCode() {
        return null;
    }
}
