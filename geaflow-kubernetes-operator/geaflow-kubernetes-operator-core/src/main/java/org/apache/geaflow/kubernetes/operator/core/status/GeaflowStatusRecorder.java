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

package org.apache.geaflow.kubernetes.operator.core.status;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractGeaflowResource;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractJobStatus;
import org.apache.geaflow.kubernetes.operator.core.model.exception.StatusConflictException;
import org.apache.geaflow.kubernetes.operator.core.util.KubernetesUtil;

/* This file is based on source code from the Flink Kubernetes Operator Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Flink Kubernetes Operator's org.apache.flink.kubernetes.operator.utils.StatusRecorder.
 */
@Slf4j
public class GeaflowStatusRecorder<CR extends AbstractGeaflowResource<?, STAT>,
    STAT extends AbstractJobStatus> {

    private static final int DEFAULT_RETRY_TIMES = 5;
    private static final long DEFAULT_INTERVAL_MS = 1000;
    private final ConcurrentHashMap<ResourceID, STAT> statusCache = new ConcurrentHashMap<>();

    /**
     * update the CR by custom logic and save the previous status into memory cache with default
     * retry params.
     *
     * @param resource the CR.
     */
    public void updateAndCacheStatus(CR resource) {
        updateAndCacheStatus(resource, DEFAULT_RETRY_TIMES, DEFAULT_INTERVAL_MS);
    }

    /**
     * update the CR by custom logic instead of the default Reconcile logic with retry.
     *
     * @param resource the CR.
     */
    @SneakyThrows
    private void updateAndCacheStatus(CR resource, int retryTimes, long intervalMs) {
        var newStatus = resource.getStatus();
        var resourceId = ResourceID.fromResource(resource);
        var prevStatus = statusCache.get(resourceId);
        if (newStatus.equals(prevStatus)) {
            log.info("Status of geaflow job {} not changed.", resource.appId());
            return;
        }
        for (int i = 0; i < retryTimes; i++) {
            try {
                replaceStatus(resource, prevStatus);
                break;
            } catch (KubernetesClientException | StatusConflictException e) {
                log.error("Error while updating status, retrying {}/{}... {}", (i + 1), retryTimes,
                    e.getMessage());
                Thread.sleep(intervalMs);
                if (i == retryTimes - 1) {
                    throw e;
                }
            }
        }
        statusCache.put(resourceId, newStatus);
    }

    /**
     * update the CR by custom logic.
     * catch the 409 error code to avoid status-update conflicts and ensure the status correctly
     * changed in this reconciliation circle.
     *
     * @param resource   the CR.
     * @param prevStatus the previous status of the CR saved in cache.
     */
    private void replaceStatus(CR resource, STAT prevStatus) {
        try {
            var updated = KubernetesUtil.getKubernetesClient().resource(resource)
                .lockResourceVersion().replaceStatus();

            // Update the resource version.
            resource.getMetadata().setResourceVersion(updated.getMetadata().getResourceVersion());
        } catch (KubernetesClientException clientException) {

            // 409 is the error code for conflicts of the resource version because of
            // multi-update in a time.
            if (clientException.getCode() == 409) {
                var currentVersion = resource.getMetadata().getResourceVersion();
                log.debug("Could not apply status update for resource version {}", currentVersion);

                var latest = KubernetesUtil.getKubernetesClient().resource(resource).get();
                var latestVersion = latest.getMetadata().getResourceVersion();

                if (latestVersion.equals(currentVersion)) {
                    // This should not happen as long as the client works consistently
                    log.error("Unable to fetch latest resource version");
                    throw clientException;
                }

                if (latest.getStatus().equals(prevStatus)) {
                    log.debug("Retrying status update for latest version {}", latestVersion);
                    resource.getMetadata().setResourceVersion(latestVersion);
                } else {
                    throw new StatusConflictException(
                        "Status have been modified externally in version " + latestVersion
                            + " Previous: " + prevStatus + " Latest: " + latest.getStatus());
                }
            } else {
                // We simply throw non conflict errors, to trigger retry with delay
                throw clientException;
            }
        }
    }

    /**
     * remove the cache of CR.
     *
     * @param resource the CR.
     */
    public void removeStatusCache(CR resource) {
        ResourceID resourceID = ResourceID.fromResource(resource);
        statusCache.remove(resourceID);
    }

    public STAT getSatatusFromCache(CR resource) {
        ResourceID resourceID = ResourceID.fromResource(resource);
        return statusCache.get(resourceID);
    }

}
