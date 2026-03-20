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

package org.apache.geaflow.common.config.keys;

import java.io.Serializable;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class FrameworkConfigKeys implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final ConfigKey ENABLE_EXTRA_OPTIMIZE = ConfigKeys
        .key("geaflow.extra.optimize.enable")
        .defaultValue(false)
        .description("union optimization, disabled by default");

    public static final ConfigKey ENABLE_EXTRA_OPTIMIZE_SINK = ConfigKeys
        .key("geaflow.extra.optimize.sink.enable")
        .defaultValue(false)
        .description("union optimization starts on the sink, disabled by default");

    public static final ConfigKey JOB_MAX_PARALLEL = ConfigKeys
        .key("geaflow.job.max.parallel")
        .defaultValue(1024)
        .description("maximum parallelism of the job, default value is 1024");

    public static final ConfigKey STREAMING_RUN_TIMES = ConfigKeys
        .key("geaflow.streaming.job.run.times")
        .defaultValue(Long.MAX_VALUE)
        .description("maximum number of job runs");

    public static final ConfigKey STREAMING_FLYING_BATCH_NUM = ConfigKeys
        .key("geaflow.streaming.flying.batch.num")
        .defaultValue(5)
        .description("the number of batches that pipelined job runs simultaneously, default value is 5");

    public static final ConfigKey BATCH_NUMBER_PER_CHECKPOINT = ConfigKeys
        .key("geaflow.batch.number.per.checkpoint")
        .defaultValue(5L)
        .description("do checkpoint every specified number of batch");

    public static final ConfigKey SYSTEM_STATE_BACKEND_TYPE = ConfigKeys
        .key("geaflow.system.state.backend.type")
        .defaultValue("ROCKSDB")
        .description("system state backend store type, e.g., [rocksdb, memory]");

    public static final ConfigKey SYSTEM_OFFSET_BACKEND_TYPE = ConfigKeys
        .key("geaflow.system.offset.backend.type")
        .defaultValue("MEMORY")
        .description("system offset backend store type, e.g., [jdbc, memory]");

    public static final ConfigKey INC_STREAM_MATERIALIZE_DISABLE = ConfigKeys
        .key("geaflow.inc.stream.materialize.disable")
        .defaultValue(false)
        .description("inc stream materialize, enabled by default");

    public static final ConfigKey SERVICE_SERVER_TYPE = ConfigKeys
        .key("geaflow.analytics.service.server.type")
        .defaultValue("analytics_rpc")
        .description("analytics service server type, e.g., [analytics_rpc, analytics_http, storage]");

    public static final ConfigKey SERVICE_SHARE_ENABLE = ConfigKeys
        .key("geaflow.analytics.service.share.enable")
        .defaultValue(false)
        .description("whether enable analytics service using share mode, default is false");

    public static final ConfigKey CLIENT_QUERY_TIMEOUT = ConfigKeys
        .key("geaflow.analytics.client.query.timeout")
        .noDefaultValue()
        .description("analytics client query max run time");

    public static final ConfigKey CLIENT_REQUEST_TIMEOUT_MILLISECOND = ConfigKeys
        .key("geaflow.analytics.client.request.timeout.millisecond")
        .defaultValue(2 * 60 * 1000)
        .description("analytics client request max run time");


    public static final ConfigKey INFER_ENV_ENABLE = ConfigKeys
        .key("geaflow.infer.env.enable")
        .defaultValue(false)
        .description("infer env enable, default is false");

    public static final ConfigKey INFER_ENV_SHARE_MEMORY_QUEUE_SIZE = ConfigKeys
        .key("geaflow.infer.env.share.memory.queue.size")
        .defaultValue(8 * 1024 * 1024)
        .description("infer env share memory queue size, default is 8 * 1024 * 1024");

    public static final ConfigKey INFER_ENV_SO_LIB_URL = ConfigKeys
        .key("geaflow.infer.env.so.lib.url")
        .noDefaultValue()
        .description("infer env so lib package oss url");

    public static final ConfigKey INFER_ENV_INIT_TIMEOUT_SEC = ConfigKeys
        .key("geaflow.infer.env.init.timeout.sec")
        .defaultValue(120)
        .description("infer env init timeout sec, default is 120");

    public static final ConfigKey INFER_ENV_SUPPRESS_LOG_ENABLE = ConfigKeys
        .key("geaflow.infer.env.suppress.log.enable")
        .defaultValue(true)
        .description("infer env suppress log enable, default is true");

    public static final ConfigKey INFER_USER_DEFINE_LIB_PATH = ConfigKeys
        .key("geaflow.infer.user.define.lib.path")
        .noDefaultValue()
        .description("infer user define lib path");

    public static final ConfigKey INFER_ENV_USER_TRANSFORM_CLASSNAME = ConfigKeys
        .key("geaflow.infer.env.user.transform.classname")
        .noDefaultValue()
        .description("infer env user custom define transform class name");

    public static final ConfigKey INFER_ENV_OSS_ACCESS_KEY = ConfigKeys
        .key("geaflow.infer.env.oss.access.key")
        .noDefaultValue()
        .description("infer env oss access key");

    public static final ConfigKey INFER_ENV_OSS_ACCESS_ID = ConfigKeys
        .key("geaflow.infer.env.oss.access.id")
        .noDefaultValue()
        .description("infer env oss access id");

    public static final ConfigKey INFER_ENV_OSS_ENDPOINT = ConfigKeys
        .key("geaflow.infer.env.oss.endpoint")
        .noDefaultValue()
        .description("infer env oss endpoint");

    public static final ConfigKey INFER_ENV_OSS_DOWNLOAD_RETRY_NUM = ConfigKeys
        .key("geaflow.infer.env.oss.download.retry.num")
        .defaultValue(3)
        .description("infer env oss download retry num, default is 3");

    public static final ConfigKey INFER_ENV_CONDA_URL = ConfigKeys
        .key("geaflow.infer.env.conda.url")
        .noDefaultValue()
        .description("infer env conda url");

    public static final ConfigKey INFER_ENV_USE_SYSTEM_PYTHON = ConfigKeys
        .key("geaflow.infer.env.use.system.python")
        .defaultValue(false)
        .description("use system Python instead of creating virtual environment");

    public static final ConfigKey INFER_ENV_SYSTEM_PYTHON_PATH = ConfigKeys
        .key("geaflow.infer.env.system.python.path")
        .noDefaultValue()
        .description("path to system Python executable (e.g., /usr/bin/python3 or /opt/homebrew/bin/python3)");

    public static final ConfigKey ASP_ENABLE = ConfigKeys
        .key("geaflow.iteration.asp.enable")
        .defaultValue(false)
        .description("whether enable iteration asp mode, disabled by default");

    public static final ConfigKey ADD_INVOKE_VIDS_EACH_ITERATION = ConfigKeys
        .key("geaflow.add.invoke.vids.each.iteration")
        .defaultValue(true)
        .description("");

    public static final ConfigKey UDF_MATERIALIZE_GRAPH_IN_FINISH = ConfigKeys
        .key("geaflow.udf.materialize.graph.in.finish")
        .defaultValue(false)
        .description("in dynmic graph, whether udf function materialize graph in finish");

}

