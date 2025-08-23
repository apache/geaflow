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

package org.apache.geaflow.dsl.connector.pulsar;

import org.apache.geaflow.dsl.connector.pulsar.PulsarTableSource.PulsarOffset;
import org.apache.geaflow.dsl.connector.pulsar.PulsarTableSource.PulsarPartition;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.junit.Assert;
import org.junit.Test;

public class PulsarTableConnectorTest {
    public static final String server = "pulsar://localhost:6650";
    public static final String topic = "persistent://test/test_pulsar_connector/non_partition_topic";
    public static final String partitionTopic = "persistent://test/test_pulsar_connector/partition_topic";


    @Test
    public void testPulsarPartition() {
        PulsarPartition partition = new PulsarPartition("topic");
        Assert.assertEquals(partition.getName(), "topic");

        PulsarPartition partition2 = new PulsarPartition("topic");
        Assert.assertEquals(partition.hashCode(), partition2.hashCode());
        Assert.assertEquals(partition, partition);
        Assert.assertEquals(partition, partition2);
        Assert.assertNotEquals(partition, null);
    }

    @Test
    public void testPulsarOffset() {
        PulsarOffset offsetByMessageId = new PulsarOffset(MessageId.earliest);
        Assert.assertEquals(offsetByMessageId.getMessageId(),
            DefaultImplementation.newMessageId(-1L, -1L, -1));
        Assert.assertEquals(offsetByMessageId.getOffset(), 0L);

        PulsarOffset offsetByTimeStamp = new PulsarOffset(11111111L);
        Assert.assertEquals(offsetByTimeStamp.getOffset(), 11111111L);
        Assert.assertNull(offsetByTimeStamp.getMessageId());

    }

}
