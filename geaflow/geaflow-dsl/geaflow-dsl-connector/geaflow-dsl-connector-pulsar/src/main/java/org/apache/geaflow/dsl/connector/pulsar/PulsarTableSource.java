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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.utils.DateTimeUtil;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.common.util.Windows;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.util.ConnectorConstants;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.pulsar.utils.PulsarConstants;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarTableSource.class);
    private Configuration tableConf;
    private String servers;
    private String topic;
    private SubscriptionType subscribeType;
    private int receiverQueueSize;
    private int negativeAckRedeliveryDelay;
    private TimeUnit timeUnit;
    private String subscriptionName;
    private SubscriptionInitialPosition subscriptionInitialPosition;
    private long windowSize;

    private transient PulsarClient pulsarClient;

    private transient Map<String, Consumer<String>> consumers;

    @Override
    public void init(Configuration conf, TableSchema tableSchema) {
        this.tableConf = conf;
        String port = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_PORT);
        String[] serversAddress = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVERS).split(",");
        servers = "pulsar://" + String.join(":" + port + ",", serversAddress);
        topic = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC);
        subscriptionName = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_NAME);

        String position = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION);
        if (position.equals("earliest")) {
            this.subscriptionInitialPosition = SubscriptionInitialPosition.Earliest;
        } else if (position.equals("latest")) {
            this.subscriptionInitialPosition = SubscriptionInitialPosition.Latest;
        } else {
            throw new GeaFlowDSLException("Invalid subscription initial position: {}", position);
        }
        subscribeType = PulsarConstants.PULSAR_SUBSCRIBE_TYPE;
        negativeAckRedeliveryDelay = PulsarConstants.PULSAR_NEGATIVE_ACK_REDELIVERY;
        timeUnit = PulsarConstants.PULSAR_NEGATIVE_ACK_REDELIVERY_UNIT;
        receiverQueueSize = PulsarConstants.PULSAR_RECEIVER_QUEUE_SIZE;

        this.windowSize = conf.getLong(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE);
        if (this.windowSize == Windows.SIZE_OF_ALL_WINDOW) {
            throw new GeaFlowDSLException("Pulsar cannot support all window");
        } else if (windowSize <= 0) {
            throw new GeaFlowDSLException("Invalid window size: {}", windowSize);
        }
    }

    private void createPulsarClient() {
        try {
            this.pulsarClient = PulsarClient.builder().serviceUrl(this.servers).build();
        } catch (PulsarClientException e) {
            throw new GeaFlowDSLException(" fail to create pulsar client, exception is {}", e);
        }
    }

    private Consumer<String> createPulsarConsumer(String partitionName) {
        Consumer<String> consumer;
        try {
            consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(partitionName)
                .subscriptionName(subscriptionName)
                .subscriptionType(subscribeType)
                .subscriptionInitialPosition(subscriptionInitialPosition)
                .negativeAckRedeliveryDelay(negativeAckRedeliveryDelay, timeUnit)
                .batchReceivePolicy(new BatchReceivePolicy.Builder().maxNumMessages((int) windowSize).build())
                .receiverQueueSize(receiverQueueSize)
                .subscribe();
        } catch (PulsarClientException e) {
            throw new GeaFlowDSLException("fail to create pulsar consumer, topic name is {}", partitionName);
        }
        return consumer;
    }

    @Override
    public void open(RuntimeContext context) {
        createPulsarClient();
        consumers = new HashMap<>();
        LOGGER.info("pulsar client created successfully");
    }

    @Override
    public List<Partition> listPartitions() {
        List<String> partitionNameList;
        try {
            partitionNameList = pulsarClient.getPartitionsForTopic(topic).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new GeaFlowDSLException("get partitions for topic fail, the topic is {}", topic);
        }
        if (partitionNameList == null) {
            throw new GeaFlowDSLException("Obtain an empty partition list through pulsarClient, the topic name is",
                topic);
        }
        return partitionNameList.stream().map(PulsarPartition::new)
            .collect(Collectors.toList());
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return DeserializerFactory.loadTextDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {

        if (windowInfo.getType() != WindowType.SIZE_TUMBLING_WINDOW) {
            throw new GeaFlowDSLException("Pulsar cannot support window type:{}", windowInfo.getType());
        }
        if (windowInfo.windowSize() <= 0) {
            throw new GeaFlowDSLException("Invalid window size: {}", windowInfo.windowSize());
        }
        windowSize = windowInfo.windowSize();
        String partitionName = partition.getName();
        Consumer<String> consumer = consumers.get(partitionName);
        if (consumer == null) {
            consumer = createPulsarConsumer(partitionName);
            consumers.put(partitionName, consumer);
        }
        assert consumer != null;
        PulsarOffset pulsarOffset;

        boolean isTimeStamp = false;
        if (startOffset.isPresent()) {
            pulsarOffset = (PulsarOffset) startOffset.get();
            if (pulsarOffset.isTimestamp()) {
                consumer.seek(pulsarOffset.getOffset());
                isTimeStamp = true;
            } else {
                consumer.seek(pulsarOffset.getMessageId());
            }
        }

        List<String> dataList = new ArrayList<>();
        Messages<String> messages = consumer.batchReceive();
        Iterator<Message<String>> iterator = messages.iterator();
        long timeOffset = 0L;
        MessageId lastMessageId = MessageId.earliest;
        while (iterator.hasNext()) {
            Message<String> message = iterator.next();
            dataList.add(message.getValue());
            timeOffset = message.getPublishTime();
            lastMessageId = message.getMessageId();
            LOGGER.info("receive message: " + message.getValue());
            LOGGER.info("object address is: " + this.hashCode());

        }

        PulsarOffset newOffset;
        if (isTimeStamp) {
            newOffset = new PulsarOffset(timeOffset);
        } else {
            newOffset = new PulsarOffset(lastMessageId);
        }
        TopicName.get(topic).getPartitionedTopicName();

        return (FetchData<T>) FetchData.createStreamFetch(dataList, newOffset, false);
    }

    @Override
    public void close() {
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            throw new GeaFlowDSLException("fail to close pulsar client, the exception is {}", e);
        }
        LOGGER.info("close pulsar client");

    }

    public static class PulsarPartition implements Partition {

        private final String topicWithPartition;

        public PulsarPartition(String topicWithPartition) {
            this.topicWithPartition = topicWithPartition;
        }

        @Override
        public String getName() {
            return topicWithPartition;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicWithPartition);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PulsarPartition)) {
                return false;
            }
            PulsarPartition that = (PulsarPartition) o;
            return Objects.equals(topicWithPartition, that.topicWithPartition);
        }
    }

    public static class PulsarOffset implements Offset {
        private final MessageId messageId;
        private final long timeStamp;

        public PulsarOffset(MessageId messageId) {
            this.messageId = messageId;
            timeStamp = 0L;
        }

        public PulsarOffset(long timeStamp) {
            this.messageId = null;
            this.timeStamp = timeStamp;
        }

        @Override
        public String humanReadable() {
            return DateTimeUtil.fromUnixTime(timeStamp, ConnectorConstants.START_TIME_FORMAT);
        }

        @Override
        public long getOffset() {
            return timeStamp;
        }

        @Override
        public boolean isTimestamp() {
            return messageId == null;
        }

        public MessageId getMessageId() {
            return messageId;
        }
    }

}
