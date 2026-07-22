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

package org.apache.geaflow.dsl.connector.doris;

public class DorisConstants {

    public static final String FORMAT_CSV = "csv";

    public static final String FORMAT_JSON = "json";

    public static final String NULL_VALUE = "\\N";

    public static final String COMMA = ",";

    public static final String COLON = ":";

    public static final String HTTP_SCHEME = "http://";

    /**
     * The Stream Load url template: http://fe_host:http_port/api/{db}/{table}/_stream_load.
     */
    public static final String STREAM_LOAD_URL_PATTERN = "%s/api/%s/%s/_stream_load";

    public static final String STREAM_LOAD_RESULT_STATUS = "Status";

    public static final String STREAM_LOAD_RESULT_MESSAGE = "Message";

    public static final String STREAM_LOAD_SUCCESS = "Success";

    public static final String STREAM_LOAD_PUBLISH_TIMEOUT = "Publish Timeout";
}
