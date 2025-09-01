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

package org.apache.geaflow.metaserver.local;

import com.google.common.collect.Lists;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.metaserver.MetaServerContext;
import org.apache.geaflow.metaserver.api.NamespaceServiceHandler;
import org.apache.geaflow.metaserver.model.protocal.MetaRequest;
import org.apache.geaflow.metaserver.model.protocal.MetaResponse;
import org.apache.geaflow.metaserver.model.protocal.request.RegisterServiceRequest;
import org.apache.geaflow.metaserver.model.protocal.response.DefaultResponse;
import org.apache.geaflow.metaserver.model.protocal.response.ServiceResponse;
import org.apache.geaflow.metaserver.service.NamespaceType;

public class DefaultServiceHandler implements NamespaceServiceHandler {

    private Map<String, HostAndPort> serviceInfoMap;

    @Override
    public void init(MetaServerContext context) {
        serviceInfoMap = new ConcurrentHashMap<>();
    }

    @Override
    public MetaResponse process(MetaRequest request) {
        switch (request.requestType()) {
            case REGISTER_SERVICE: {
                RegisterServiceRequest registerServiceRequest = (RegisterServiceRequest) request;
                serviceInfoMap.put(registerServiceRequest.getContainerId(),
                    registerServiceRequest.getInfo());
                return new DefaultResponse(true);
            }
            case QUERY_ALL_SERVICE: {
                return new ServiceResponse(Lists.newArrayList(serviceInfoMap.values()));
            }
            default:
                return new DefaultResponse(false, "not support request " + request.requestType());
        }
    }

    @Override
    public NamespaceType namespaceType() {
        return NamespaceType.DEFAULT;
    }
}
