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

package org.apache.geaflow.file;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class FileConfigKeys {

    protected static final int CORE_NUM = Runtime.getRuntime().availableProcessors();
    public static final ConfigKey PERSISTENT_TYPE = ConfigKeys
        .key("geaflow.file.persistent.type")
        .defaultValue("LOCAL")
        .description("geaflow file persistent type");

    public static final ConfigKey PERSISTENT_THREAD_SIZE = ConfigKeys
        .key("geaflow.file.persistent.thread.size")
        .defaultValue(CORE_NUM)
        .description("geaflow file persistent thread size");

    public static final ConfigKey USER_NAME = ConfigKeys
        .key("geaflow.file.persistent.user.name")
        .defaultValue("geaflow")
        .description("geaflow file base url");

    public static final ConfigKey JSON_CONFIG = ConfigKeys
        .key("geaflow.file.persistent.config.json")
        .defaultValue("")
        .description("geaflow json config");

    public static final ConfigKey ROOT = ConfigKeys
        .key("geaflow.file.persistent.root")
        .defaultValue("/geaflow/chk")
        .description("geaflow file persistent root path");

    /**
     * oss config.
     */
    public static final ConfigKey OSS_BUCKET_NAME = ConfigKeys
        .key("geaflow.file.oss.bucket.name")
        .defaultValue(null)
        .description("oss bucket name");

    public static final ConfigKey OSS_ENDPOINT = ConfigKeys
        .key("geaflow.file.oss.endpoint")
        .defaultValue(null)
        .description("oss endpoint");

    public static final ConfigKey OSS_ACCESS_ID = ConfigKeys
        .key("geaflow.file.oss.access.id")
        .defaultValue(null)
        .description("oss access id");

    public static final ConfigKey OSS_SECRET_KEY = ConfigKeys
        .key("geaflow.file.oss.secret.key")
        .defaultValue(null)
        .description("oss secret key");


    public static final ConfigKey S3_BUCKET_NAME = ConfigKeys
        .key("geaflow.file.s3.bucket.name")
        .defaultValue(null)
        .description("s3 bucket name");

    public static final ConfigKey S3_ENDPOINT = ConfigKeys
        .key("geaflow.file.s3.endpoint")
        .defaultValue(null)
        .description("s3 endpoint");

    public static final ConfigKey S3_ACCESS_KEY_ID = ConfigKeys
        .key("geaflow.file.s3.access.key.id")
        .defaultValue(null)
        .description("s3 access key id");

    public static final ConfigKey S3_ACCESS_KEY = ConfigKeys
        .key("geaflow.file.s3.access.key")
        .defaultValue(null)
        .description("s3 access key");

    public static final ConfigKey S3_MIN_PART_SIZE = ConfigKeys
        .key("geaflow.file.s3.min.part.size")
        .defaultValue(5242880L)
        .description("s3 input minimum part size in bytes");

    public static final ConfigKey S3_INPUT_STREAM_CHUNK_SIZE = ConfigKeys
        .key("geaflow.file.s3.input.stream.chunk.size")
        .defaultValue(1048576)
        .description("s3 input stream chunk size in bytes");


    public static final ConfigKey S3_REGION = ConfigKeys
        .key("geaflow.file.s3.region")
        .defaultValue("CN_NORTH_1")
        .description("s3 region");
}
