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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTableSink.class);

    private Configuration tableConf;
    private StructType schema;
    private String separator;
    private String servers;
    private String valueSerializerClass;
    private String topic;
    private Properties props;
    private int writeTimeout;

    private transient KafkaProducer<String, String> producer;

    @Override
    public void init(Configuration conf, StructType schema) {
        LOGGER.info("prepare with config: {}, \n schema: {}", conf, schema);
        this.tableConf = conf;
        this.schema = schema;

        this.servers = conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_SERVERS);
        topic = conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_TOPIC);
        this.valueSerializerClass = KafkaConstants.KAFKA_VALUE_SERIALIZER_CLASS;
        this.separator = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
    }

    @Override
    public void open(RuntimeContext context) {
        props = new Properties();
        props.setProperty(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, servers);
        props.setProperty(KafkaConstants.KAFKA_KEY_SERIALIZER, valueSerializerClass);
        props.setProperty(KafkaConstants.KAFKA_VALUE_SERIALIZER, valueSerializerClass);
        if (context.getConfiguration().contains(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_CLIENT_ID)) {
            String useClientId = context.getConfiguration()
                .getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_CLIENT_ID);
            props.put(KafkaConstants.KAFKA_CLIENT_ID, useClientId);
        }

        writeTimeout = context.getConfiguration()
            .getInteger(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_DATA_OPERATION_TIMEOUT);

        producer = new KafkaProducer<>(props);

        Properties consumerProps = new Properties();
        consumerProps.putAll(props);
        String valueDeserializerClassString =
            KafkaConstants.KAFKA_VALUE_DESERIALIZER_CLASS;
        consumerProps.setProperty(KafkaConstants.KAFKA_KEY_DESERIALIZER,
            valueDeserializerClassString);
        consumerProps.setProperty(KafkaConstants.KAFKA_VALUE_DESERIALIZER,
            valueDeserializerClassString);
        KafkaConsumer<String, String> tmpConsumer = new KafkaConsumer<>(consumerProps);
        Map<String, List<PartitionInfo>> topic2PartitionInfo = tmpConsumer.listTopics();
        tmpConsumer.close();
        if (!topic2PartitionInfo.containsKey(topic)) {
            producer.close();
            producer = null;
            throw new GeaFlowDSLException("Topic: [{}] has not been created.", topic);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        Object[] values = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            values[i] = row.getField(i, schema.getType(i));
        }
        ProducerRecord<String, String> record = createRecord(values);
        try {
            producer.send(record).get(writeTimeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IOException("When write kafka.", e);
        }
    }

    private void flush() {
        LOGGER.info("flush");
        if (producer != null) {
            producer.flush();
        } else {
            LOGGER.warn("Producer is null.");
        }
    }

    protected ProducerRecord<String, String> createRecord(Object[] flushValues) {
        return new ProducerRecord<>(topic, StringUtils.join(flushValues, separator));
    }

    @Override
    public void finish() throws IOException {
        flush();
    }

    @Override
    public void close() {
        LOGGER.info("close");
        flush();
        if (producer != null) {
            producer.close();
        }
    }
}
