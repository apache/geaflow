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

package org.apache.geaflow.dsl.connector.elasticsearch;

public class ElasticsearchConstants {

    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final String DEFAULT_SCROLL_TIMEOUT = "60s";

    public static final int DEFAULT_CONNECTION_TIMEOUT = 1000;

    public static final int DEFAULT_SOCKET_TIMEOUT = 30000;

    public static final int DEFAULT_SEARCH_SIZE = 1000;

    public static final String ES_SCHEMA_SUFFIX = "://";

    public static final String ES_HTTP_SCHEME = "http";

    public static final String ES_HTTPS_SCHEME = "https";

    public static final String ES_SPLIT_COMMA = ",";

    public static final String ES_SPLIT_COLON = ";";

}
