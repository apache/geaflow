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

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.EVICTED_POD_LABELS;

import io.fabric8.kubernetes.api.model.Pod;
import java.util.Map;
import org.apache.geaflow.cluster.k8s.handler.PodHandlerRegistry.EventKind;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.stats.model.ExceptionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PodEvictHandler extends AbstractPodHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PodEvictHandler.class);
    private final Map<String, String> evictLabels;
    private int totalCount;

    public PodEvictHandler(Configuration configuration) {
        this.evictLabels = KubernetesUtils.getPairsConf(configuration, EVICTED_POD_LABELS);
    }

    @Override
    public void handle(Pod pod) {
        Map<String, String> labels = pod.getMetadata().getLabels();
        for (Map.Entry<String, String> entry : evictLabels.entrySet()) {
            String key = entry.getKey();
            if (labels.get(key) != null && labels.get(key).equalsIgnoreCase(entry.getValue())) {
                String componentId = KubernetesUtils.extractComponentId(pod);
                String message = String.format(
                    "Pod #%s %s will be removed, label: %s annotations: %s, total removed: %s",
                    componentId, pod.getMetadata().getName(), key,
                    pod.getMetadata().getAnnotations(), ++totalCount);
                LOG.info(message);

                PodEvent event = new PodEvent(pod, EventKind.POD_EVICTION);
                notifyListeners(event);
                reportPodEvent(event, ExceptionLevel.WARN, message);
                break;
            }
        }
    }
}