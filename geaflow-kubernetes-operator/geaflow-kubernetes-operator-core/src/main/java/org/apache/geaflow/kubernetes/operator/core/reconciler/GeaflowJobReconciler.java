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

package org.apache.geaflow.kubernetes.operator.core.reconciler;

import com.alibaba.fastjson2.JSON;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.kubernetes.operator.core.job.GeaflowJobManager;
import org.apache.geaflow.kubernetes.operator.core.job.JobCRCache;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJobSpec;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJobStatus;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.UserSpec;
import org.apache.geaflow.kubernetes.operator.core.model.exception.GeaflowDeploymentException;
import org.apache.geaflow.kubernetes.operator.core.model.job.ComponentType;
import org.apache.geaflow.kubernetes.operator.core.model.job.GeaflowClientState;
import org.apache.geaflow.kubernetes.operator.core.model.job.JobState;
import org.apache.geaflow.kubernetes.operator.core.status.GeaflowStatusRecorder;
import org.apache.geaflow.kubernetes.operator.core.util.EventSourceUtil;
import org.apache.geaflow.kubernetes.operator.core.util.GeaflowJobUtil;
import org.apache.geaflow.kubernetes.operator.core.util.KubernetesUtil;
import org.apache.geaflow.kubernetes.operator.core.util.ReconcilerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

@ControllerConfiguration(namespaces = Constants.WATCH_CURRENT_NAMESPACE, name = "geaflowjob")
@Slf4j
public class GeaflowJobReconciler implements Reconciler<GeaflowJob>, ErrorStatusHandler<GeaflowJob>,
    Cleaner<GeaflowJob>, EventSourceInitializer<GeaflowJob> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaflowJobReconciler.class);

    private final KubernetesClient kubernetesClient;

    private final GeaflowJobManager geaflowJobManager;

    private final JobCRCache jobCRCache;

    private final GeaflowStatusRecorder<GeaflowJob, GeaflowJobStatus> geaflowStatusRecorder;

    public GeaflowJobReconciler(KubernetesClient kubernetesClient,
                                GeaflowJobManager geaflowJobManager,
                                JobCRCache jobCRCache) {
        this.kubernetesClient = kubernetesClient;
        this.geaflowJobManager = geaflowJobManager;
        this.jobCRCache = jobCRCache;
        this.geaflowStatusRecorder = new GeaflowStatusRecorder<>();
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<GeaflowJob> context) {

        var masterServiceEventSource = EventSourceUtil.getGeaflowMasterServiceEventSource(context);
        var masterDeploymentEventSource = EventSourceUtil.getGeaflowMasterDeploymentEventSource(
            context);
        var clientPodEventSource = EventSourceUtil.getGeaflowClientPodEventSource(context);
        return EventSourceInitializer.nameEventSources(clientPodEventSource,
            masterServiceEventSource, masterDeploymentEventSource);
    }

    @Override
    public UpdateControl<GeaflowJob> reconcile(GeaflowJob geaflowJob, Context<GeaflowJob> context) {
        log.info("Start reconciliation of geaflow job: {}.", geaflowJob.appId());
        jobCRCache.updateJobCache(geaflowJob);

        GeaflowJob prevJob = new GeaflowJob();
        BeanUtils.copyProperties(geaflowJob, prevJob);

        // Check client and master state if job is SUBMITTED or RUNNING.
        checkClientState(geaflowJob, context);
        checkMasterAvailable(geaflowJob, context);

        // Check spec diff and judge if it needs to redeploy the job.
        checkSpecDiffAndDeploy(geaflowJob, context);

        // Backup the latest spec after this reconciliation
        geaflowJob.getStatus().setLastReconciledSpec(JSON.toJSONString(geaflowJob.getSpec()));
        geaflowStatusRecorder.updateAndCacheStatus(geaflowJob);
        log.info("Finish reconciliation of geaflow job {}", geaflowJob.appId());
        jobCRCache.updateJobCache(geaflowJob);
        return ReconcilerUtil.buildUpdateControl(prevJob, geaflowJob);
    }

    private void deploy(GeaflowJob geaflowJob, Context<GeaflowJob> context) {
        try {
            if (geaflowJob.getStatus().getClientState() != GeaflowClientState.EXITED
                || geaflowJob.getStatus().getMasterState() != GeaflowClientState.EXITED) {
                log.info("Destroy cluster and wait for its completion before redeploy a new one.");
                geaflowJobManager.destroyJob(geaflowJob, context);
                KubernetesUtil.waitForClusterDestroyed(geaflowJob.appId(), 30);
            }
            setJobIdIfNecessary(geaflowJob);
            geaflowJobManager.deployJob(geaflowJob, context);
            updateGeaflowJobState(geaflowJob, JobState.SUBMITTED);
            geaflowJob.getStatus().setClientState(GeaflowClientState.DEPLOYED_NOT_READY);
            geaflowJob.getStatus().setErrorMessage(null);
        } catch (Exception e) {
            LOGGER.info("Create geaflow job failed, {}", e.getMessage(), e);
            String errorMsg = String.format("Create client pod of job %s failed: %s",
                geaflowJob.appId(), e.getMessage());
            throw new GeaflowDeploymentException(errorMsg, "PodNotCreated", ComponentType.client);
        }
    }

    private void setJobIdIfNecessary(GeaflowJob resource) {
        Long userJobUid = Optional.ofNullable(resource.getSpec().getUserSpec())
            .map(UserSpec::getJobUid).orElse(null);
        Long currentJobUid = resource.getStatus().getJobUid();
        if (userJobUid != null) {
            resource.getStatus().setJobUid(userJobUid);
        } else if (currentJobUid == null) {
            // Only generate uid when current job uid is null.
            resource.getStatus().setJobUid(GeaflowJobUtil.generateJobUid());
        }
        LOGGER.info("Set jobUid {} to GeaFlow job {}. Previous jobUid: {}.",
            resource.getStatus().getJobUid(), resource.appId(), currentJobUid);
    }

    private void checkSpecDiffAndDeploy(GeaflowJob geaflowJob, Context<GeaflowJob> context) {

        // Create service and other secondary resources when job first submit with empty state.
        if (beforeFirstDeploy(geaflowJob)) {
            log.info("Deploying Geaflow job {} with spec: {}.", geaflowJob.appId(), geaflowJob);
            deploy(geaflowJob, context);
            updateGeaflowJobState(geaflowJob, JobState.SUBMITTED);
            return;
        }

        JobState jobState = geaflowJob.getStatus().getState();
        JobState cachedState = Optional.ofNullable(
                geaflowStatusRecorder.getSatatusFromCache(geaflowJob)).map(GeaflowJobStatus::getState)
            .orElse(null);

        // Step1. If there is spec diff, then cancel job and update to REDEPLOYING status. This
        // will trigger another reconciliation in the next cycle.
        if (jobState != JobState.REDEPLOYING && checkSpecDiff(geaflowJob)) {
            // Redeploy in the next turn.
            log.info("Spec of geaflow job {} has been changed, suspend the job and restart later."
                    + " Old spec: {}. New spec: {}",
                geaflowJob.appId(), geaflowJob.getStatus().getLastReconciledSpec(), geaflowJob.getSpec());
            geaflowJobManager.destroyJob(geaflowJob, context);
            // Wait for the cluster destroy completely
            KubernetesUtil.waitForClusterDestroyed(geaflowJob.appId(), 30);
            updateGeaflowJobState(geaflowJob, JobState.REDEPLOYING);
            geaflowJob.getStatus().setClientState(GeaflowClientState.EXITED);
            geaflowJob.getStatus().setMasterState(GeaflowClientState.EXITED);
            return;
        }

        // Step2, redeploy the REDEPLOYING job in the next reconciliation cycle.
        if (jobState == JobState.REDEPLOYING && cachedState == JobState.REDEPLOYING) {
            log.info("Spec of geaflow job {} has been changed, restart now.", geaflowJob.appId());
            deploy(geaflowJob, context);
            updateGeaflowJobState(geaflowJob, JobState.SUBMITTED);
        }
    }

    private boolean beforeFirstDeploy(GeaflowJob geaflowJob) {
        return (geaflowJob.getStatus() == null || (geaflowJob.getStatus().getState() == null
            && geaflowJob.getStatus().getLastReconciledSpec() == null));
    }

    private boolean checkSpecDiff(GeaflowJob geaflowJob) {
        // TODO 做一层Filter逻辑，判别哪些改动需要进行重启
        GeaflowJobSpec desiredSpec = geaflowJob.getSpec();
        GeaflowJobSpec lastSpec = JSON.parseObject(geaflowJob.getStatus().getLastReconciledSpec(),
            GeaflowJobSpec.class);
        if (!desiredSpec.equals(lastSpec)) {
            log.info("Spec change of job {} detected. Previous: {}, Desired: {}",
                geaflowJob.appId(), lastSpec, desiredSpec);
            return true;
        }
        return false;
    }

    private void checkClientState(GeaflowJob geaflowJob, Context<GeaflowJob> context) {
        GeaflowJobStatus cachedState = geaflowStatusRecorder.getSatatusFromCache(geaflowJob);
        if (cachedState != null && cachedState.getState() == JobState.REDEPLOYING) {
            return;
        }
        JobState previousJobState = geaflowJob.getStatus().getState();
        GeaflowClientState previousClientState = geaflowJob.getStatus().getClientState();
        if (previousJobState == JobState.SUBMITTED) {
            Set<Pod> clientPods = context.getSecondaryResources(Pod.class);
            if (CollectionUtils.isEmpty(clientPods)) {
                throw new GeaflowDeploymentException("Client pod is deleted while submitting.",
                    "PodDeleted", ComponentType.client);
            }
            if (previousClientState == GeaflowClientState.DEPLOYED_NOT_READY) {
                KubernetesUtil.checkPodBackOff(clientPods, ComponentType.client);
                if (KubernetesUtil.isPodStateRunning(clientPods)) {
                    log.info("Client pod is running now, change client status of job {} to RUNNING",
                        geaflowJob.appId());
                    geaflowJob.getStatus().setClientState(GeaflowClientState.RUNNING);
                }
                return;
            }
        }
        if (previousJobState == JobState.RUNNING
            && previousClientState == GeaflowClientState.RUNNING) {
            Set<Pod> clientPods = context.getSecondaryResources(Pod.class);
            if (CollectionUtils.isEmpty(clientPods)) {
                log.info("Client pod of job {} has finished, changing the status to EXITED.",
                    geaflowJob.appId());
                geaflowJob.getStatus().setClientState(GeaflowClientState.EXITED);
            }
        }

    }

    private void checkMasterAvailable(GeaflowJob geaflowJob, Context<GeaflowJob> context) {

        GeaflowJobStatus cachedState = geaflowStatusRecorder.getSatatusFromCache(geaflowJob);
        if (cachedState != null && cachedState.getState() == JobState.REDEPLOYING) {
            return;
        }
        String appId = geaflowJob.appId();
        JobState jobState = geaflowJob.getStatus().getState();
        GeaflowClientState clientState = geaflowJob.getStatus().getClientState();
        if (jobState == JobState.SUBMITTED && clientState == GeaflowClientState.RUNNING) {
            Optional<Deployment> masterDeployment = context.getSecondaryResource(Deployment.class);
            if (masterDeployment.isPresent()) {
                geaflowJob.getStatus().setMasterState(GeaflowClientState.DEPLOYED_NOT_READY);
                KubernetesUtil.checkMasterPodBackoff(appId);
                log.info(
                    "Master deployment and pods are successfully deployed, changing state of job "
                        + "{} " + "to RUNNING.", appId);
                geaflowJob.getStatus().setMasterState(GeaflowClientState.RUNNING);
                updateGeaflowJobState(geaflowJob, JobState.RUNNING);
            }
        }
        if (jobState == JobState.RUNNING) {
            Optional<Deployment> masterDeployment = context.getSecondaryResource(Deployment.class);
            if (masterDeployment.isEmpty()) {
                throw new GeaflowDeploymentException("Master Deployment is deleted while running.",
                    "MasterDeleted", ComponentType.master);
            }
        }
    }

    private void updateGeaflowJobState(GeaflowJob geaflowJob, JobState jobState) {
        GeaflowJobStatus jobStatus = Optional.ofNullable(geaflowJob.getStatus())
            .orElse(new GeaflowJobStatus());
        jobStatus.setState(jobState);
        geaflowJob.setStatus(jobStatus);
        log.warn("Geaflow job {} is updated to {}", geaflowJob.appId(), jobState);
    }

    @Override
    public ErrorStatusUpdateControl<GeaflowJob> updateErrorStatus(GeaflowJob resource,
                                                                  Context<GeaflowJob> context,
                                                                  Exception e) {
        log.info("Start handling error when reconcile job {}, {}", resource.appId(),
            e.getMessage());
        jobCRCache.updateJobCache(resource);
        if (e instanceof GeaflowDeploymentException) {
            GeaflowClientState clientState = GeaflowClientState.EXITED;
            GeaflowClientState masterState = GeaflowClientState.EXITED;
            if (((GeaflowDeploymentException) e).getComponentType() == ComponentType.client) {
                clientState = GeaflowClientState.ERROR;
            } else if (((GeaflowDeploymentException) e).getComponentType()
                == ComponentType.master) {
                masterState = GeaflowClientState.ERROR;
            }
            resource.getStatus().setMasterState(masterState);
            resource.getStatus().setClientState(clientState);
            updateGeaflowJobState(resource, JobState.FAILED);
            log.warn("Geaflow job {} is changed to FAILED caused by deployment failure: {}",
                resource.appId(), e.getMessage());
        }
        resource.getStatus().setErrorMessage("Error: " + e.getMessage());
        geaflowStatusRecorder.updateAndCacheStatus(resource);
        log.info("Finish handling error when reconcile job {}", resource.appId());
        jobCRCache.updateJobCache(resource);
        return ErrorStatusUpdateControl.noStatusUpdate();
    }

    @Override
    public DeleteControl cleanup(GeaflowJob resource, Context<GeaflowJob> context) {
        // Destroy geaflow job caused by user deletion.
        geaflowJobManager.destroyJob(resource, context);
        log.info("Geaflow job {} is deleted", resource.appId());
        geaflowStatusRecorder.removeStatusCache(resource);
        jobCRCache.invalidateCache(resource.appId());
        return DeleteControl.defaultDelete();
    }
}