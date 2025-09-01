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

package org.apache.geaflow.metaserver.model.protocal;

import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.metaserver.model.protocal.request.QueryAllServiceRequest;
import org.apache.geaflow.metaserver.model.protocal.request.RegisterServiceRequest;
import org.apache.geaflow.metaserver.model.protocal.request.RequestPBConverter;
import org.apache.geaflow.metaserver.service.NamespaceType;
import org.apache.geaflow.rpc.proto.MetaServer.ServiceRequestPb;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetaRequestTest {

    @Test
    public void testQueryAllServiceRequest() {

        QueryAllServiceRequest queryAllServiceRequest = new QueryAllServiceRequest();
        ServiceRequestPb request = RequestPBConverter.convert(queryAllServiceRequest);
        QueryAllServiceRequest serviceRequest = (QueryAllServiceRequest) RequestPBConverter.convert(
            request);

        Assert.assertSame(serviceRequest.requestType(), MetaRequestType.QUERY_ALL_SERVICE);
    }

    @Test
    public void testRegisterServiceRequest() {
        RegisterServiceRequest registerServiceRequest =
            new RegisterServiceRequest(NamespaceType.STATE_SERVICE, "1",
                new HostAndPort("127.0.0.1", 1024));
        ServiceRequestPb request = RequestPBConverter.convert(registerServiceRequest);

        RegisterServiceRequest serviceRequest = (RegisterServiceRequest) RequestPBConverter.convert(
            request);

        Assert.assertEquals(registerServiceRequest.getContainerId(),
            serviceRequest.getContainerId());
        Assert.assertEquals(registerServiceRequest.getInfo(), serviceRequest.getInfo());
        Assert.assertEquals(registerServiceRequest.requestType(), serviceRequest.requestType());
        Assert.assertEquals(registerServiceRequest.namespaceType(), NamespaceType.STATE_SERVICE);
    }

}
