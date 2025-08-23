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

package org.apache.geaflow.console.core.service.security;

import org.apache.geaflow.console.common.dal.dao.EdgeDao;
import org.apache.geaflow.console.common.dal.dao.FunctionDao;
import org.apache.geaflow.console.common.dal.dao.GraphDao;
import org.apache.geaflow.console.common.dal.dao.InstanceDao;
import org.apache.geaflow.console.common.dal.dao.JobDao;
import org.apache.geaflow.console.common.dal.dao.TableDao;
import org.apache.geaflow.console.common.dal.dao.TaskDao;
import org.apache.geaflow.console.common.dal.dao.VertexDao;
import org.apache.geaflow.console.common.dal.dao.ViewDao;
import org.apache.geaflow.console.common.dal.entity.EdgeEntity;
import org.apache.geaflow.console.common.dal.entity.FunctionEntity;
import org.apache.geaflow.console.common.dal.entity.GraphEntity;
import org.apache.geaflow.console.common.dal.entity.InstanceEntity;
import org.apache.geaflow.console.common.dal.entity.JobEntity;
import org.apache.geaflow.console.common.dal.entity.TableEntity;
import org.apache.geaflow.console.common.dal.entity.TaskEntity;
import org.apache.geaflow.console.common.dal.entity.VertexEntity;
import org.apache.geaflow.console.common.dal.entity.ViewEntity;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.security.resource.GeaflowResource;
import org.apache.geaflow.console.core.model.security.resource.GraphResource;
import org.apache.geaflow.console.core.model.security.resource.InstanceResource;
import org.apache.geaflow.console.core.model.security.resource.JobResource;
import org.apache.geaflow.console.core.model.security.resource.TableResource;
import org.apache.geaflow.console.core.model.security.resource.TaskResource;
import org.apache.geaflow.console.core.model.security.resource.TenantResource;
import org.apache.geaflow.console.core.model.security.resource.ViewResource;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResourceFactory implements InitializingBean {

    private static ResourceFactory INSTANCE;

    @Autowired
    private InstanceDao instanceDao;

    @Autowired
    private TableDao tableDao;

    @Autowired
    private ViewDao viewDao;

    @Autowired
    private FunctionDao functionDao;

    @Autowired
    private VertexDao vertexDao;

    @Autowired
    private EdgeDao edgeDao;

    @Autowired
    private GraphDao graphDao;

    @Autowired
    private JobDao jobDao;

    @Autowired
    private TaskDao taskDao;

    protected static ResourceFactory getInstance() {
        if (INSTANCE == null) {
            throw new GeaflowException("{} is not ready", ResourceFactory.class.getSimpleName());
        }
        return INSTANCE;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        INSTANCE = this;
    }

    public <T extends GeaflowResource> T build(GeaflowResourceType resourceType, String resourceId) {
        GeaflowResource resource;
        try {
            switch (resourceType) {
                case TENANT:
                    resource = new TenantResource(resourceId);
                    break;

                case INSTANCE:
                    InstanceEntity instance = instanceDao.get(resourceId);
                    resource = new InstanceResource(instance.getTenantId(), instance.getId());
                    break;

                case TABLE:
                    TableEntity table = tableDao.get(resourceId);
                    resource = new TableResource(table.getTenantId(), table.getInstanceId(), table.getId());
                    break;

                case VIEW:
                    ViewEntity view = viewDao.get(resourceId);
                    resource = new ViewResource(view.getTenantId(), view.getInstanceId(), view.getId());
                    break;

                case FUNCTION:
                    FunctionEntity function = functionDao.get(resourceId);
                    resource = new ViewResource(function.getTenantId(), function.getInstanceId(), function.getId());
                    break;

                case VERTEX:
                    VertexEntity vertex = vertexDao.get(resourceId);
                    resource = new GraphResource(vertex.getTenantId(), vertex.getInstanceId(), vertex.getId());
                    break;

                case EDGE:
                    EdgeEntity edge = edgeDao.get(resourceId);
                    resource = new GraphResource(edge.getTenantId(), edge.getInstanceId(), edge.getId());
                    break;

                case GRAPH:
                    GraphEntity graph = graphDao.get(resourceId);
                    resource = new GraphResource(graph.getTenantId(), graph.getInstanceId(), graph.getId());
                    break;

                case JOB:
                    JobEntity job = jobDao.get(resourceId);
                    resource = new JobResource(job.getTenantId(), job.getInstanceId(), job.getId());
                    break;

                case TASK:
                    TaskEntity task = taskDao.get(resourceId);
                    resource = new TaskResource(build(GeaflowResourceType.JOB, task.getJobId()), task.getId());
                    break;

                default:
                    throw new GeaflowException("Resource type {} not supported", resourceType);
            }

            return (T) resource;

        } catch (Exception e) {
            throw new GeaflowIllegalException("Build resource {} {} failed", resourceType, resourceId, e);
        }
    }
}
