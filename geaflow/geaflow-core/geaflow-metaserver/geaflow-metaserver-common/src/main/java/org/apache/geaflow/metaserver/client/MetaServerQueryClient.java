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

package org.apache.geaflow.metaserver.client;

import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.metaserver.model.protocal.request.QueryAllServiceRequest;
import org.apache.geaflow.metaserver.model.protocal.response.ServiceResponse;
import org.apache.geaflow.metaserver.service.NamespaceType;

public class MetaServerQueryClient extends BaseClient {

    private static MetaServerQueryClient client;

    public MetaServerQueryClient(Configuration configuration) {
        super(configuration);
    }

    public static synchronized MetaServerQueryClient getClient(Configuration configuration) {
        if (client == null) {
            client = new MetaServerQueryClient(configuration);
        }
        return client;
    }


    public List<HostAndPort> queryAllServices(NamespaceType namespaceType) {
        ServiceResponse response = process(new QueryAllServiceRequest(namespaceType));
        if (!response.isSuccess()) {
            throw new GeaflowRuntimeException(response.getMessage());
        }
        return response.getServiceInfos();
    }

    @Override
    public void close() {
        super.close();
        client = null;
    }
}
