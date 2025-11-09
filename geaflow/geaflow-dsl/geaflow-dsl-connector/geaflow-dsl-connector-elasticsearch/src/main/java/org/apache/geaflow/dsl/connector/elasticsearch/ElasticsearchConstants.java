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

    public static final int DEFAULT_MAX_RETRIES = 3;

    public static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = 10000L; // 10 seconds

    public static final long DEFAULT_SOCKET_TIMEOUT_MILLIS = 60000L; // 60 seconds

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 10000; // 10 seconds (milliseconds as int)

    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 60000; // 60 seconds (milliseconds as int)

    public static final int DEFAULT_MAX_CONNECTIONS = 100;

    public static final String DEFAULT_ID_FIELD = "_id";

    public static final String DEFAULT_SCROLL_TIMEOUT = "5m";

    public static final int DEFAULT_SCROLL_SIZE = 1000;
}
