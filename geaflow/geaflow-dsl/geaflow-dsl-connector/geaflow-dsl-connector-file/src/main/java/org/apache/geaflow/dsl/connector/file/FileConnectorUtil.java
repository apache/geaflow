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

package org.apache.geaflow.dsl.connector.file;

import static org.apache.geaflow.dsl.connector.file.FileConstants.PREFIX_S3_RESOURCE;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConnectorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileConnectorUtil.class);

    private static final String HADOOP_HOME = "HADOOP_HOME";

    public static String getPartitionFileName(int taskIndex) {
        return "partition_" + taskIndex;
    }

    public static FileSystem getHdfsFileSystem(Configuration conf) {
        org.apache.hadoop.conf.Configuration hadoopConf = toHadoopConf(conf);
        FileSystem fileSystem;
        try {
            fileSystem = FileSystem.newInstance(hadoopConf);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Cannot init hdfs file system.", e);
        }
        return fileSystem;
    }

    public static org.apache.hadoop.conf.Configuration toHadoopConf(Configuration conf) {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        String hadoopConfPath = System.getenv(HADOOP_HOME);
        if (!StringUtils.isEmpty(hadoopConfPath)) {
            LOGGER.info("find hadoop home at: {}", hadoopConfPath);
            hadoopConf.addResource(new Path(hadoopConfPath + "/etc/hadoop/core-site.xml"));
            hadoopConf.addResource(new Path(hadoopConfPath + "/etc/hadoop/hdfs-site.xml"));
        }
        if (conf.contains(FileConfigKeys.JSON_CONFIG)) {
            String userConfigStr = conf.getString(FileConfigKeys.JSON_CONFIG);
            Map<String, String> userConfig = GsonUtil.parse(userConfigStr);
            if (userConfig != null) {
                for (Map.Entry<String, String> entry : userConfig.entrySet()) {
                    hadoopConf.set(entry.getKey(), entry.getValue());
                }
            }
        }
        if (conf.contains("fs.defaultFS")) {
            hadoopConf.set("fs.defaultFS", conf.getString("fs.defaultFS"));
        }
        return hadoopConf;
    }

    public static AWSCredentials getS3Credentials(Configuration conf) {
        String accessKey = conf.getString(FileConstants.S3_ACCESS_KEY);
        String secretKey = conf.getString(FileConstants.S3_SECRET_KEY);
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        return credentials;
    }

    public static String getS3ServiceEndpoint(Configuration conf) {
        return conf.getString(FileConstants.S3_SERVICE_ENDPOINT);
    }

    public static String[] getFileUri(String path) {
        String uri = path.substring(PREFIX_S3_RESOURCE.length());
        return uri.split("/");
    }

    public static String getBucket(String path) {
        String[] paths = getFileUri(path);
        return paths[0];
    }

    public static String getKey(String path) {
        String[] paths = getFileUri(path);
        String[] keys = new String[paths.length - 1];
        System.arraycopy(paths, 1, keys, 0, keys.length);
        return String.join("/", keys);
    }

}
