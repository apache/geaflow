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

package org.apache.geaflow.metrics.common;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricNameFormatterTest {

    @Test
    public void testMetricNameFormatter() {
        int opId = 1;
        String opInputMetricName = MetricNameFormatter.inputTpsMetricName(this.getClass(), opId);
        Assert.assertEquals(opInputMetricName, "inputTps.MetricNameFormatterTest[1](N)");
        String opOutputMetricName = MetricNameFormatter.outputTpsMetricName(this.getClass(), opId);
        Assert.assertEquals(opOutputMetricName, "outputTps.MetricNameFormatterTest[1](N)");
        String opRtMetricName = MetricNameFormatter.rtMetricName(this.getClass(), opId);
        Assert.assertEquals(opRtMetricName, "processRt.MetricNameFormatterTest[1](us)");
        String vertexTpsMetricName = MetricNameFormatter.vertexTpsMetricName(this.getClass(), opId);
        Assert.assertEquals(vertexTpsMetricName, "vertexTps.MetricNameFormatterTest[1](N)");
        String edgeTpsMetricName = MetricNameFormatter.edgeTpsMetricName(this.getClass(), opId);
        Assert.assertEquals(edgeTpsMetricName, "edgeTps.MetricNameFormatterTest[1](N)");
        String iterationFinishMetricName = MetricNameFormatter.iterationFinishMetricName(this.getClass(), opId, 0);
        Assert.assertEquals(iterationFinishMetricName, "iteration.MetricNameFormatterTest[1:0](ms)");
        String iterationMsgMetricName = MetricNameFormatter.iterationMsgMetricName(this.getClass(), opId);
        Assert.assertEquals(iterationMsgMetricName, "iterationMsgTps.MetricNameFormatterTest[1](N)");
    }

}
