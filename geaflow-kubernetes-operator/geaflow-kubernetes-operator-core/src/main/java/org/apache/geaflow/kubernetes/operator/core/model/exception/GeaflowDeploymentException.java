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

package org.apache.geaflow.kubernetes.operator.core.model.exception;

import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import org.apache.geaflow.kubernetes.operator.core.model.job.ComponentType;

public class GeaflowDeploymentException extends RuntimeException {

    private final String reason;

    private final ComponentType componentType;

    public GeaflowDeploymentException(ContainerStateWaiting stateWaiting,
                                      ComponentType componentType) {
        this(stateWaiting.getMessage(), stateWaiting.getReason(), componentType);
    }

    public GeaflowDeploymentException(String message, String reason, ComponentType componentType) {
        super(message);
        this.reason = reason;
        this.componentType = componentType;
    }

    public String getReason() {
        return reason;
    }

    public ComponentType getComponentType() {
        return componentType;
    }

    @Override
    public String getMessage() {
        return String.format("「Reason: %s」「Component: %s」「Message: %s」", getReason(),
            getComponentType(), super.getMessage());
    }
}
