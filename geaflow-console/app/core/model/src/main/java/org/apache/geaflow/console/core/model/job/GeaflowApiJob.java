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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;
import org.apache.geaflow.console.core.model.code.GeaflowCode;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.job.GeaflowTransferJob.StructMapping;

@Getter
@Setter
public abstract class GeaflowApiJob extends GeaflowJob {

    @Setter
    protected GeaflowRemoteFile jarPackage;

    @Setter
    private String entryClass;

    public GeaflowApiJob(GeaflowJobType type) {
        super(type);
    }

    public List<GeaflowFunction> getFunctions() {
        return functions;
    }

    public List<StructMapping> getStructMappings() {
        return new ArrayList<>();
    }

    @Override
    public boolean isApiJob() {
        return true;
    }

    @Override
    public GeaflowCode getUserCode() {
        return null;
    }
}
