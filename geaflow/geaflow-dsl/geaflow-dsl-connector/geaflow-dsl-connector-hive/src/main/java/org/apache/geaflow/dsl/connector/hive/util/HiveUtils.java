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

package org.apache.geaflow.dsl.connector.hive.util;

import java.io.IOException;
import java.util.Map;
import org.apache.geaflow.common.utils.ClassUtil;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveUtils {

    public static String INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";

    public static String THRIFT_URIS = "hive.metastore.uris";

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtils.class);

    public static HiveConf getHiveConfig(Map<String, String> config) {
        HiveConf hiveConf = new HiveConf();
        for (String key : config.keySet()) {
            hiveConf.set(key, config.get(key));
        }
        return hiveConf;
    }

    public static JobConf getJobConf(StorageDescriptor storageDescriptor) {
        JobConf jobConf = new JobConf();
        jobConf.set(INPUT_DIR, storageDescriptor.getLocation());
        return jobConf;
    }

    public static InputFormat<Writable, Writable> createInputFormat(StorageDescriptor storageDescriptor) {
        JobConf jobConf = getJobConf(storageDescriptor);
        jobConf.set(INPUT_DIR, storageDescriptor.getLocation());
        try {
            return ClassUtil.newInstance(storageDescriptor.getInputFormat());
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate the hadoop input format", e);
        }
    }

    public static InputSplit[] createInputSplits(StorageDescriptor storageDescriptor,
                                                 InputFormat inputFormat,
                                                 int splitNumPerPartition) throws IOException {
        JobConf jobConf = getJobConf(storageDescriptor);
        ReflectionUtils.setConf(inputFormat, jobConf);
        if (inputFormat instanceof Configurable) {
            ((Configurable) inputFormat).setConf(jobConf);
        } else if (inputFormat instanceof JobConfigurable) {
            ((JobConfigurable) inputFormat).configure(jobConf);
        }
        return inputFormat.getSplits(jobConf, splitNumPerPartition);
    }

    public static Reporter createDummyReporter() {
        return new Reporter() {
            @Override
            public void setStatus(String status) {

            }

            @Override
            public Counter getCounter(Enum<?> name) {
                return new Counter();
            }

            @Override
            public Counter getCounter(String group, String name) {
                return new Counter();
            }

            @Override
            public void incrCounter(Enum<?> key, long amount) {

            }

            @Override
            public void incrCounter(String group, String counter, long amount) {

            }

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public void progress() {

            }
        };
    }

}
