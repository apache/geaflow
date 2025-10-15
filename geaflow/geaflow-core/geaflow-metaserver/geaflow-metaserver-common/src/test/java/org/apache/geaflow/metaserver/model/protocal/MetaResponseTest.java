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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.metaserver.model.protocal.response.DefaultResponse;
import org.apache.geaflow.metaserver.model.protocal.response.ResponsePBConverter;
import org.apache.geaflow.metaserver.model.protocal.response.ServiceResponse;
import org.apache.geaflow.rpc.proto.MetaServer.ServiceResultPb;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetaResponseTest {

    @Test
    public void testDefaultResponse() {
        DefaultResponse defaultResponse = new DefaultResponse(true);
        ServiceResultPb result = ResponsePBConverter.convert(defaultResponse);
        DefaultResponse response = (DefaultResponse) ResponsePBConverter.convert(result);

        Assert.assertTrue(response.isSuccess());

        defaultResponse = new DefaultResponse(false, "error");
        result = ResponsePBConverter.convert(defaultResponse);
        response = (DefaultResponse) ResponsePBConverter.convert(result);

        Assert.assertFalse(response.isSuccess());
        Assert.assertEquals(response.getMessage(), "error");
    }

    @Test
    public void testServiceResponse() {
        ServiceResponse serviceResponse = new ServiceResponse(false, "error");
        ServiceResultPb result = ResponsePBConverter.convert(serviceResponse);
        ServiceResponse response = (ServiceResponse) ResponsePBConverter.convert(result);

        Assert.assertFalse(response.isSuccess());
        Assert.assertEquals(response.getMessage(), "error");

        List<HostAndPort> HostAndPortList = Lists.newArrayList(new HostAndPort("127.0.0.1", 1024),
            new HostAndPort("127.0.0.1", 1025));

        serviceResponse = new ServiceResponse(HostAndPortList);
        result = ResponsePBConverter.convert(serviceResponse);
        response = (ServiceResponse) ResponsePBConverter.convert(result);

        Assert.assertEquals(serviceResponse.getServiceInfos(), response.getServiceInfos());
        Assert.assertEquals(serviceResponse.isSuccess(), response.isSuccess());

    }

}
