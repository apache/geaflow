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

package org.apache.geaflow.cluster.k8s.handler;

import io.fabric8.kubernetes.api.model.Pod;
import org.apache.geaflow.cluster.k8s.handler.PodHandlerRegistry.EventKind;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.stats.model.ExceptionLevel;

public class PodAddedHandler extends AbstractPodHandler {

    @Override
    public void handle(Pod pod) {
        String componentId = KubernetesUtils.extractComponentId(pod);
        if (componentId != null) {
            String addMessage = String.format("Pod #%s %s is created.",
                componentId, pod.getMetadata().getName());
            PodEvent event = new PodEvent(pod, EventKind.POD_ADDED);
            reportPodEvent(event, ExceptionLevel.INFO, addMessage);
        }
    }
}
