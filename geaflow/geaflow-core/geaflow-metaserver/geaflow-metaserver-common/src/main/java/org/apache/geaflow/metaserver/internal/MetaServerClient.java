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

package org.apache.geaflow.metaserver.internal;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.metaserver.client.BaseClient;
import org.apache.geaflow.metaserver.model.protocal.request.RegisterServiceRequest;
import org.apache.geaflow.metaserver.model.protocal.response.DefaultResponse;
import org.apache.geaflow.metaserver.service.NamespaceType;

public class MetaServerClient extends BaseClient {

    private static MetaServerClient client;

    public MetaServerClient() {

    }

    public MetaServerClient(Configuration configuration) {
        super(configuration);
    }

    public static synchronized MetaServerClient getClient(Configuration configuration) {
        if (client == null) {
            client = new MetaServerClient(configuration);
        }
        return client;
    }

    public void registerService(NamespaceType namespaceType, String containerId,
                                HostAndPort serviceInfo) {
        DefaultResponse response = process(
            new RegisterServiceRequest(namespaceType, containerId, serviceInfo));
        if (!response.isSuccess()) {
            throw new GeaflowRuntimeException(response.getMessage());
        }
    }

    @Override
    public void close() {
        super.close();
        client = null;
    }
}
