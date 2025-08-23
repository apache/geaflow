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

package org.apache.geaflow.console.test;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.console.biz.shared.convert.JobViewConverter;
import org.apache.geaflow.console.biz.shared.view.JobView;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.job.GeaflowIntegrateJob;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.job.GeaflowTransferJob.StructMapping;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class IntegrationTest {

    @Test
    public void testGenerateCode() {
        String structString = "[\n"
            + "  {\n"
            + "    \"tableName\": \"t1\",\n"
            + "    \"structName\": \"v1\",\n"
            + "    \"fieldMappings\": [\n"
            + "      {\"tableFieldName\":\"t_id\",\"structFieldName\":\"v_id\"},\n"
            + "      {\"tableFieldName\":\"t_id\",\"structFieldName\":\"v_id\"},\n"
            + "      {\"tableFieldName\":\"t_name\",\"structFieldName\":\"v_name\"}\n"
            + "    ]\n"
            + "  },\n"
            + "   {\n"
            + "    \"tableName\": \"t2\",\n"
            + "    \"structName\": \"v2\",\n"
            + "    \"fieldMappings\": [\n"
            + "      {\"tableFieldName\":\"t_id\",\"structFieldName\":\"v_id\"},\n"
            + "      {\"tableFieldName\":\"t_name\",\"structFieldName\":\"v_name\"},\n"
            + "      {\"tableFieldName\":\"t_name\",\"structFieldName\":\"v_name\"},\n"
            + "      {\"tableFieldName\":\"t_id\",\"structFieldName\":\"v_id2\"}\n"
            + "    ]\n"
            + "  },\n"
            + "   {\n"
            + "    \"tableName\": \"edgeTable\",\n"
            + "    \"structName\": \"e1\",\n"
            + "    \"fieldMappings\": [\n"
            + "      {\"tableFieldName\":\"t_srcId\",\"structFieldName\":\"e_srcId\"},\n"
            + "      {\"tableFieldName\":\"t_targetId\",\"structFieldName\":\"e_targetId\"}\n"
            + "    ]\n"
            + "  }\n"
            + "]";

        JobView jobView = new JobView();
        jobView.setStructMappings(structString);
        jobView.setType(GeaflowJobType.INTEGRATE);
        JobViewConverter jobViewConverter = new JobViewConverter();
        GeaflowJob job = jobViewConverter.convert(jobView, new ArrayList<>(), new ArrayList<>(), null, null);
        List<StructMapping> structMappings = job.getStructMappings();
        Assert.assertEquals(structMappings.get(0).getFieldMappings().size(), 2);
        Assert.assertEquals(structMappings.get(1).getFieldMappings().size(), 3);

        job.setGraph(Lists.newArrayList(new GeaflowGraph("g1", null)));
        String text = ((GeaflowIntegrateJob) job).generateCode().getText();
        Assert.assertEquals(text, "USE GRAPH g1;\n"
            + "\n"
            + "insert into g1(\n"
            + "    v1.v_id,\n"
            + "    v1.v_name\n"
            + ") select\n"
            + "    t_id,\n"
            + "    t_name\n"
            + "from t1;\n"
            + "    \n"
            + "insert into g1(\n"
            + "    v2.v_id,\n"
            + "    v2.v_name,\n"
            + "    v2.v_id2\n"
            + ") select\n"
            + "    t_id,\n"
            + "    t_name,\n"
            + "    t_id\n"
            + "from t2;\n"
            + "    \n"
            + "insert into g1(\n"
            + "    e1.e_srcId,\n"
            + "    e1.e_targetId\n"
            + ") select\n"
            + "    t_srcId,\n"
            + "    t_targetId\n"
            + "from edgeTable;\n"
            + "    \n");
    }


}
