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

package org.apache.geaflow.dsl.runtime.testenv;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.connector.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.mutable.ArraySeq;

public class KafkaTestEnv {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestEnv.class);

    private static final KafkaTestEnv INSTANCE = new KafkaTestEnv();

    private static SecurityManager securityManager;

    private KafkaTestEnv() {
    }

    public static KafkaTestEnv get() {
        return INSTANCE;
    }

    private KafkaServer server;

    private ExecutorService zkServer;

    private static final String KAFKA_LOGS_PATH = "/tmp/kafka-logs/dsl-kafka-connector-test";

    private static final String ZOOKEEPER_LOGS_PATH = "/tmp/zookeeper-kafka-test";

    public void startZkServer() throws IOException {
        cleanZk();
        if (zkServer == null) {
            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(
                "zookeeperServerThread" + "-%d").build();
            ExecutorService singleThreadPool = new ThreadPoolExecutor(5, 5, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(5), namedThreadFactory);
            singleThreadPool.execute(() -> {
                String clientPort = "2181";
                String tickTime = "16000";
                ServerConfig config = new ServerConfig();
                config.parse(new String[]{clientPort, ZOOKEEPER_LOGS_PATH, tickTime});
                ZooKeeperServerMain zk = new ZooKeeperServerMain();
                try {
                    zk.runFromConfig(config);
                } catch (Exception e) {
                    throw new GeaFlowDSLException("Error in run zookeeper.", e);
                }
            });
            zkServer = singleThreadPool;
        }
        SleepUtils.sleepSecond(10);
    }

    public void startKafkaServer() throws IOException {
        cleanKafka();
        if (server != null) {
            LOGGER.info("server has been established.");
            return;
        }
        startZkServer();
        Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("log.dirs", KAFKA_LOGS_PATH);
        props.put("num.partitions", "1");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("offsets.topic.replication.factor", "1");
        props.put("zookeeper.connection.timeout.ms", "10000");
        props.put("retries", "5");
        server = new KafkaServer(new KafkaConfig(props), Time.SYSTEM, Option.<String>empty(), new ArraySeq<>(0));
        LOGGER.info("valid kafka server config.");
        server.startup();
        SleepUtils.sleepMilliSecond(1000);
        LOGGER.info("server startup.");
    }

    public void createTopic(String topic) {
        Properties props = new Properties();
        props.setProperty(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
        props.setProperty(KafkaConstants.KAFKA_KEY_DESERIALIZER,
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(KafkaConstants.KAFKA_VALUE_DESERIALIZER,
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(KafkaConstants.KAFKA_MAX_POLL_RECORDS,
            String.valueOf(500));
        props.setProperty(KafkaConstants.KAFKA_GROUP_ID, "geaflow-dsl-kafka-source-default-group-id");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Map<String, List<PartitionInfo>> topic2PartitionInfo = consumer.listTopics();
        if (!topic2PartitionInfo.containsKey(topic)) {
            Properties producerProps = new Properties();
            producerProps.setProperty(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
            producerProps.setProperty(KafkaConstants.KAFKA_KEY_SERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.setProperty(KafkaConstants.KAFKA_VALUE_SERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
            producer.partitionsFor(topic);
            producer.close();
        }
        consumer.close();
    }

    public void shutdownKafkaServer() throws IOException {
        shutdownKafka();
        cleanKafka();
        cleanZookeeper();
    }

    private void shutdownKafka() throws IOException {
        if (server == null) {
            LOGGER.warn("null server cannot be shutdownKafka.");
        } else {
            server.shutdown();
            server = null;
            cleanKafka();
            LOGGER.info("server shutdownKafka.");
        }
    }

    private void cleanZookeeper() throws IOException {
        if (zkServer != null) {
            zkServer.shutdownNow();
            zkServer = null;
            cleanZk();
            LOGGER.info("zk closed.");
        }
    }

    private void cleanZk() throws IOException {
        File file = new File(ZOOKEEPER_LOGS_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
    }

    private void cleanKafka() throws IOException {
        File file = new File(KAFKA_LOGS_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
    }

    public static void before() {
        securityManager = System.getSecurityManager();
        System.setSecurityManager(new SystemExitIgnoreSecurityManager());
    }

    public static void after() {
        System.setSecurityManager(securityManager);
    }

    private static class SystemExitIgnoreSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
        }

        @Override
        public void checkExit(int status) {
            super.checkExit(status);
            LOGGER.info("check exit {}", status);
            throw new RuntimeException("throw exception instead of exit process");
        }
    }
}
