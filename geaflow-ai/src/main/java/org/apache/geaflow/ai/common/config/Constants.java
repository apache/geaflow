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

package org.apache.geaflow.ai.common.config;

public class Constants {

    public static String MODEL_CONTEXT_ROLE_USER = "user";
    public static String PREFIX_V = "V";
    public static String PREFIX_E = "E";
    public static String PREFIX_GRAPH = "GRAPH";
    public static String PREFIX_TMP_SESSION = "TmpSession-";
    public static String PREFIX_SRC_ID = "srcId";
    public static String PREFIX_DST_ID = "dstId";

    public static int HTTP_CALL_TIMEOUT_SECONDS = 300;
    public static int HTTP_CONNECT_TIMEOUT_SECONDS = 300;
    public static int HTTP_READ_TIMEOUT_SECONDS = 300;
    public static int HTTP_WRITE_TIMEOUT_SECONDS = 300;

    public static int MODEL_CLIENT_RETRY_TIMES = 10;
    public static int MODEL_CLIENT_RETRY_INTERVAL_MS = 3000;

    public static int EMBEDDING_INDEX_STORE_BATCH_SIZE = 32;
    public static int EMBEDDING_INDEX_STORE_REPORT_SIZE = 100;
    public static int EMBEDDING_INDEX_STORE_FLUSH_WRITE_SIZE = 1024;
    public static int EMBEDDING_INDEX_STORE_SPLIT_TEXT_CHUNK_SIZE = 128;

    public static double EMBEDDING_OPERATE_DEFAULT_THRESHOLD = 0.5;
    public static int EMBEDDING_OPERATE_DEFAULT_TOPN = 50;
    public static int GRAPH_SEARCH_STORE_DEFAULT_TOPN = 30;

    public static String CONSOLIDATE_KEYWORD_RELATION_LABEL = "consolidate_keyword_edge";
    public static String PREFIX_COMMON_KEYWORDS = "common_keywords";
}
