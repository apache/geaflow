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

package org.apache.geaflow.kubernetes.operator.core.model.customresource;

import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import lombok.Getter;

@Group("geaflow.antgroup.com")
@Version("v1")
@Getter
public class GeaflowJob extends
    AbstractGeaflowResource<GeaflowJobSpec, GeaflowJobStatus> {

    @Override
    public GeaflowJobStatus initStatus() {
        return new GeaflowJobStatus();
    }

    @Override
    public String toString() {
        return "GeaflowJob{" + "spec=" + spec + ", status=" + status + '}';
    }
}
