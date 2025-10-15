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

package org.apache.geaflow.kubernetes.operator.core.util;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.kubernetes.operator.core.model.GeaflowMappers;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractGeaflowResource;

public class EventSourceUtil {

    private static final String CLIENT_LABEL = getComponentLabel(
        K8SConstants.LABEL_COMPONENT_CLIENT);

    private static final String MASTER_LABEL = getComponentLabel(
        K8SConstants.LABEL_COMPONENT_MASTER);


    public static <R extends AbstractGeaflowResource> InformerEventSource<Service, R> getGeaflowMasterServiceEventSource(
        EventSourceContext<R> context) {

        var configuration = InformerConfiguration.from(Service.class, context)
            .withLabelSelector(MASTER_LABEL)
            // .withSecondaryToPrimaryMapper(Mappers.fromLabel(K8SConstants.LABEL_APP_KEY))
            .withSecondaryToPrimaryMapper(
                GeaflowMappers.fromServiceSpecSelector(K8SConstants.LABEL_APP_KEY))
            .followNamespaceChanges(true).build();

        return new InformerEventSource<>(configuration, context);
    }

    public static <R extends AbstractGeaflowResource> InformerEventSource<Deployment, R> getGeaflowMasterDeploymentEventSource(
        EventSourceContext<R> context) {

        var configuration = InformerConfiguration.from(Deployment.class, context)
            .withLabelSelector(MASTER_LABEL)
            .withSecondaryToPrimaryMapper(Mappers.fromLabel(K8SConstants.LABEL_APP_KEY))
            .followNamespaceChanges(true).build();

        return new InformerEventSource<>(configuration, context);
    }

    public static <R extends AbstractGeaflowResource> InformerEventSource<Pod, R> getGeaflowClientPodEventSource(
        EventSourceContext<R> context) {

        var configuration = InformerConfiguration.from(Pod.class, context)
            .withLabelSelector(CLIENT_LABEL)
            .withSecondaryToPrimaryMapper(Mappers.fromLabel(K8SConstants.LABEL_APP_KEY))
            .followNamespaceChanges(true).build();

        return new InformerEventSource<>(configuration, context);
    }

    private static String getComponentLabel(String componentType) {
        return Map.of(K8SConstants.LABEL_COMPONENT_KEY, componentType).entrySet().stream()
            .map(Object::toString).collect(Collectors.joining(","));
    }

}
