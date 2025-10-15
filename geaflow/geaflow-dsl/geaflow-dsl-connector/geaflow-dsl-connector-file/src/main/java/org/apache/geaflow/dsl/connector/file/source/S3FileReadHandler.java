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

package org.apache.geaflow.dsl.connector.file.source;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class S3FileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileReadHandler.class);

    protected String path;

    protected AWSCredentials credentials;
    protected String serviceEndpoint;

    protected AmazonS3 s3;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        super.init(tableConf, tableSchema, path);
        this.path = path;
        this.credentials = FileConnectorUtil.getS3Credentials(tableConf);
        this.serviceEndpoint = FileConnectorUtil.getS3ServiceEndpoint(tableConf);
        s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return credentials;
                }

                @Override
                public void refresh() {
                }
            })
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
            .build();
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        List<Partition> partitions = new ArrayList<>();
        try {
            ListObjectsV2Result result = s3.listObjectsV2(
                FileConnectorUtil.getBucket(path),
                FileConnectorUtil.getKey(path)
            );


            result.getObjectSummaries()
                .forEach((S3ObjectSummary obj) -> {
                    ResourceFileSplit split = new ResourceFileSplit(obj.getBucketName(), obj.getKey());
                    split.setS3(s3);
                    partitions.add(split);
                });
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Cannot get partitions with path: " + path, e);
        }
        return partitions;
    }

    public static class ResourceFileSplit extends FileTableSource.FileSplit {

        private AmazonS3 s3;

        private String object;

        private String key;

        public ResourceFileSplit(String baseDir, String relativePath) {
            super(baseDir, relativePath);
            this.object = baseDir;
            this.key = relativePath;
        }

        void setS3(AmazonS3 s3) {
            this.s3 = s3;
        }

        @Override
        public InputStream openStream(Configuration conf) throws IOException {
            S3Object obj = s3.getObject(object, key);
            return obj.getObjectContent();
        }
    }

}
