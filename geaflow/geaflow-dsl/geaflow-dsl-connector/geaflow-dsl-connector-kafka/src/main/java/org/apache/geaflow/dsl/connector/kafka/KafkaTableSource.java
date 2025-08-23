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
package org.apache.geaflow.dsl.connector.kafka;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.utils.DateTimeUtil;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.AbstractTableSource;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.util.ConnectorConstants;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.TimeFetchWindow;
import org.apache.geaflow.dsl.connector.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaTableSource extends AbstractTableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTableSource.class);

    private static final Duration OPERATION_TIMEOUT =
        Duration.ofSeconds(KafkaConstants.KAFKA_OPERATION_TIMEOUT_SECONDS);

    private String topic;
    private long startTimeMs;
    private Properties props;
    private Duration pollTimeout;

    private transient KafkaConsumer<String, String> consumer;

    @Override
    public void init(Configuration conf, TableSchema tableSchema) {

        final String servers =
            conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_SERVERS);
        topic = conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_TOPIC);
        pollTimeout = Duration.ofSeconds(conf.getInteger(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_DATA_OPERATION_TIMEOUT));
        final String groupId = conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_GROUP_ID);
        final String valueDeserializerClassString = KafkaConstants.KAFKA_VALUE_DESERIALIZER_CLASS;
        final String startTimeStr = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_START_TIME,
            (String) ConnectorConfigKeys.GEAFLOW_DSL_START_TIME.getDefaultValue());
        if (startTimeStr.equalsIgnoreCase(KafkaConstants.KAFKA_BEGIN)) {
            startTimeMs = 0;
        } else if (startTimeStr.equalsIgnoreCase(KafkaConstants.KAFKA_LATEST)) {
            startTimeMs = Long.MAX_VALUE;
        } else {
            startTimeMs = DateTimeUtil.toUnixTime(startTimeStr, ConnectorConstants.START_TIME_FORMAT);
        }
        if (conf.contains(DSLConfigKeys.GEAFLOW_DSL_TIME_WINDOW_SIZE)) {
            Preconditions.checkState(startTimeMs > 0, "Time window need unified start time! Please set config:%s", ConnectorConfigKeys.GEAFLOW_DSL_START_TIME.getKey());
        }
        int pullSize = conf.getInteger(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_PULL_BATCH_SIZE);
        if (pullSize <= 0) {
            throw new GeaFlowDSLException("Config {} is illegal:{}", KafkaConfigKeys.GEAFLOW_DSL_KAFKA_PULL_BATCH_SIZE, pullSize);
        }
        this.props = new Properties();
        props.setProperty(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, servers);
        props.setProperty(KafkaConstants.KAFKA_KEY_DESERIALIZER,
            valueDeserializerClassString);
        props.setProperty(KafkaConstants.KAFKA_VALUE_DESERIALIZER,
            valueDeserializerClassString);
        props.setProperty(KafkaConstants.KAFKA_MAX_POLL_RECORDS,
            String.valueOf(pullSize));
        props.setProperty(KafkaConstants.KAFKA_GROUP_ID, groupId);
        if (conf.contains(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_CLIENT_ID)) {
            String useClientId = conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_CLIENT_ID);
            props.put(KafkaConstants.KAFKA_CLIENT_ID, useClientId);
        }
        LOGGER.info("open kafka, servers is: {}, topic is:{}, config is:{}, schema is: {}",
            servers, topic, conf, tableSchema);
    }

    @Override
    public void open(RuntimeContext context) {
        consumer = new KafkaConsumer<>(props);
        LOGGER.info("consumer opened, topic: {}", topic);
    }

    @Override
    public List<Partition> listPartitions() {
        KafkaConsumer tmpConsumer = new KafkaConsumer<>(props);
        List<PartitionInfo> partitions = tmpConsumer.partitionsFor(topic, OPERATION_TIMEOUT);
        tmpConsumer.close();
        return partitions.stream().map(
            partition -> new KafkaPartition(topic, partition.partition())
        ).collect(Collectors.toList());
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return DeserializerFactory.loadDeserializer(conf);
    }

    private Tuple<TopicPartition, Long> fetchPrepare(Partition partition, Optional<Offset> startOffset) {
        KafkaPartition kafkaPartition = (KafkaPartition) partition;
        TopicPartition topicPartition = new TopicPartition(kafkaPartition.getTopic(),
            kafkaPartition.getPartition());
        Set<TopicPartition> singletonPartition = Collections.singleton(topicPartition);

        Set<TopicPartition> currentAssignment = consumer.assignment();
        if (currentAssignment.size() != 1 || !currentAssignment.contains(topicPartition)) {
            consumer.assign(singletonPartition);
        }

        KafkaOffset reqKafkaOffset;
        if (startOffset.isPresent()) {
            reqKafkaOffset = (KafkaOffset) startOffset.get();
            consumer.seek(topicPartition, reqKafkaOffset.getKafkaOffset());
            return Tuple.of(topicPartition, reqKafkaOffset.getKafkaOffset());
        } else {
            if (startTimeMs == 0) {
                Map<TopicPartition, Long> partition2Offset =
                    consumer.beginningOffsets(singletonPartition, OPERATION_TIMEOUT);
                Long beginningOffset = partition2Offset.get(topicPartition);
                if (beginningOffset == null) {
                    throw new GeaFlowDSLException("Cannot get beginning offset for partition: {}, "
                        + "startTime: {}.", topicPartition, startTimeMs);
                } else {
                    consumer.seek(topicPartition, beginningOffset);
                }
                return Tuple.of(topicPartition, beginningOffset);
            } else if (startTimeMs == Long.MAX_VALUE) {
                Map<TopicPartition, Long> endOffsets =
                    consumer.endOffsets(Collections.singletonList(topicPartition), OPERATION_TIMEOUT);
                Long beginningOffset = endOffsets.get(topicPartition);
                if (beginningOffset == null) {
                    throw new GeaFlowDSLException("Cannot get beginning offset for partition: {}, "
                        + "startTime: {}.", topicPartition, startTimeMs);
                } else {
                    consumer.seek(topicPartition, beginningOffset);
                }
                return Tuple.of(topicPartition, beginningOffset);
            } else {
                Map<TopicPartition, OffsetAndTimestamp> partitionOffset =
                    consumer.offsetsForTimes(Collections.singletonMap(topicPartition,
                        startTimeMs / 1000), OPERATION_TIMEOUT);
                OffsetAndTimestamp offset = partitionOffset.get(topicPartition);
                if (offset == null) {
                    throw new GeaFlowDSLException("Cannot get offset for partition: {}, "
                        + "startTime: {}.", topicPartition, startTimeMs);
                } else {
                    consumer.seek(topicPartition, offset.offset());
                }
                return Tuple.of(topicPartition, offset.offset());
            }
        }
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  SizeFetchWindow windowInfo) throws IOException {
        TopicPartition topicPartition = fetchPrepare(partition, startOffset).f0;

        List<String> dataList = new ArrayList<>();
        long responseMaxTimestamp = -1;
        while (dataList.size() < windowInfo.windowSize()) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, String> record : records) {
                assert record.topic().equals(this.topic) : "Illegal topic";
                dataList.add(record.value());
                if (record.timestamp() > responseMaxTimestamp) {
                    responseMaxTimestamp = record.timestamp();
                }
            }
        }
        //reload cursor
        long nextOffset = consumer.position(topicPartition, OPERATION_TIMEOUT);
        KafkaOffset nextKafkaOffset;
        if (responseMaxTimestamp >= 0) {
            nextKafkaOffset = new KafkaOffset(nextOffset, responseMaxTimestamp);
        } else {
            nextKafkaOffset = new KafkaOffset(nextOffset, System.currentTimeMillis());
        }
        return (FetchData<T>) FetchData.createStreamFetch(dataList, nextKafkaOffset, false);
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, TimeFetchWindow windowInfo) throws IOException {
        Tuple<TopicPartition, Long> partitionWithOffset = fetchPrepare(partition, startOffset);
        long windowStartTimeMs = windowInfo.getStartWindowTime(startTimeMs);
        long windowEndTimeMs = windowInfo.getEndWindowTime(startTimeMs);
        OffsetAndTimestamp windowStartOffset = queryTimesOffset(partitionWithOffset.f0, windowStartTimeMs);
        if (windowStartOffset == null || windowStartOffset.timestamp() >= windowEndTimeMs) {
            // no data in current window, skip!
            KafkaOffset offset = new KafkaOffset(partitionWithOffset.f1, windowEndTimeMs);
            return (FetchData<T>) FetchData.createStreamFetch(Collections.EMPTY_LIST, offset, false);
        }
        List<String> dataList = new ArrayList<>();
        long responseMaxTimestamp = -1;
        while (responseMaxTimestamp < windowEndTimeMs && responseMaxTimestamp < System.currentTimeMillis()) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    dataList.add(record.value());
                    if (record.timestamp() > responseMaxTimestamp) {
                        responseMaxTimestamp = record.timestamp();
                    }
                }
            } else if (windowEndTimeMs > System.currentTimeMillis()) {
                // no new msg, break;
                break;
            }
        }
        //reload cursor
        long nextOffset = consumer.position(partitionWithOffset.f0, OPERATION_TIMEOUT);
        KafkaOffset nextKafkaOffset;
        if (responseMaxTimestamp >= 0) {
            nextKafkaOffset = new KafkaOffset(nextOffset, responseMaxTimestamp);
        } else {
            nextKafkaOffset = new KafkaOffset(nextOffset, System.currentTimeMillis());
        }
        return (FetchData<T>) FetchData.createStreamFetch(dataList, nextKafkaOffset, false);
    }

    private OffsetAndTimestamp queryTimesOffset(TopicPartition topicPartition, long timestampInMs) {
        Map<TopicPartition, OffsetAndTimestamp> partitionOffset = consumer.offsetsForTimes(
            Collections.singletonMap(topicPartition, timestampInMs / 1000), OPERATION_TIMEOUT);
        return partitionOffset.get(topicPartition);
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        LOGGER.info("close");
    }


    public static class KafkaPartition implements Partition {

        private final String topic;
        private final int partitionId;

        public KafkaPartition(String topic, int partitionId) {
            this.topic = topic;
            this.partitionId = partitionId;
        }

        @Override
        public String getName() {
            return topic + "-" + partitionId;
        }

        @Override
        public void setIndex(int index, int parallel) {
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partitionId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KafkaPartition)) {
                return false;
            }
            KafkaPartition that = (KafkaPartition) o;
            return Objects.equals(topic, that.topic) && Objects.equals(
                partitionId, that.partitionId);
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partitionId;
        }
    }


    public static class KafkaOffset implements Offset {

        private final long offset;

        private final long humanReadableTime;

        public KafkaOffset(long offset, long humanReadableTime) {
            this.offset = offset;
            this.humanReadableTime = humanReadableTime;
        }

        @Override
        public String humanReadable() {
            return DateTimeUtil.fromUnixTime(humanReadableTime, ConnectorConstants.START_TIME_FORMAT);
        }

        @Override
        public long getOffset() {
            return humanReadableTime;
        }

        public long getKafkaOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return true;
        }
    }
}
