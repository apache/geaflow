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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.cluster.k8s.handler;

import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.k8s.handler.PodHandlerRegistry.EventKind;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.stats.model.ExceptionLevel;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PodOOMHandler extends AbstractPodHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PodOOMHandler.class);
    private static final String OOM_KILLED_KEY = "OOMKilled";
    private static final Exception POD_OOM_MSG = new Exception("pod overused memory");

    private final DateTimeFormatter parser = ISODateTimeFormat.dateTimeNoMillis();
    private final Map<Long, List<Tuple<String, Exception>>> exceptions;
    private int totalOOMCount;

    public PodOOMHandler() {
        this.totalOOMCount = 0;
        this.exceptions = new HashMap<>();
    }

    @Override
    public void handle(Pod pod) {
        if (pod.getStatus() != null && !pod.getStatus().getContainerStatuses().isEmpty()) {
            for (ContainerStatus containerStatus : pod.getStatus().getContainerStatuses()) {
                ContainerState state = containerStatus.getState();
                if (state != null && state.getTerminated() != null
                    && state.getTerminated().getReason() != null
                    && state.getTerminated().getFinishedAt() != null) {
                    if (state.getTerminated().getReason().contains(OOM_KILLED_KEY)) {
                        String finishTime = containerStatus.getState().getTerminated()
                            .getFinishedAt();
                        DateTime parsed;
                        try {
                            parsed = parser.parseDateTime(finishTime);
                        } catch (Exception e) {
                            LOGGER.error("Failed to parse finish time: {}", finishTime, e);
                            return;
                        }
                        long exceptionTime = parsed.getMillis();
                        List<Tuple<String, Exception>> oldList = exceptions.get(exceptionTime);

                        boolean added = true;
                        String componentId = KubernetesUtils.extractComponentId(pod);
                        Tuple<String, Exception> newException = new Tuple<>(componentId,
                            POD_OOM_MSG);
                        if (oldList == null) {
                            exceptions.computeIfAbsent(exceptionTime, k -> new ArrayList<>())
                                .add(newException);
                        } else {
                            if (exists(newException, oldList)) {
                                added = false;
                            } else {
                                oldList.add(newException);
                            }
                        }

                        if (added) {
                            totalOOMCount++;
                            LOGGER.info("Pod #{} {} oom killed at {}, totally: {}", componentId,
                                pod.getMetadata().getName(), parsed, totalOOMCount);

                            PodEvent oomEvent = new PodEvent(pod, EventKind.POD_OOM, exceptionTime);
                            notifyListeners(oomEvent);

                            String errMsg = String.format("pod %s oom killed at %s",
                                pod.getMetadata().getName(), parsed);
                            reportPodEvent(oomEvent, ExceptionLevel.ERROR, errMsg);
                        }
                    }
                }
            }
        }
    }

    private boolean exists(Tuple<String, Exception> target,
                           List<Tuple<String, Exception>> exceptions) {
        for (Tuple<String, Exception> e : exceptions) {
            if (e.f0.equals(target.f0)) {
                return true;
            }
        }
        return false;
    }

}