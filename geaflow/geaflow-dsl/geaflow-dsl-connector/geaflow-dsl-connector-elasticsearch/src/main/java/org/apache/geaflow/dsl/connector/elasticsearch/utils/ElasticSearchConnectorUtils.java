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

package org.apache.geaflow.dsl.connector.elasticsearch.utils;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

/**
 * @author wanhanbo
 * @version ElasticSearchConnectorUtils.java, v 0.1 2025年11月15日 22:14 wanhanbo
 */
public class ElasticSearchConnectorUtils {

    public static  HttpHost[] toHttpHosts(String[] hostStrings) {
        HttpHost[] hosts = new HttpHost[hostStrings.length];
        for (int i = 0; i < hostStrings.length; i++) {
            String host = hostStrings[i].trim();
            if (!host.startsWith("http://") && !host.startsWith("https://")) {
                host = "http://" + host; //default HTTP
            }
            hosts[i] = HttpHost.create(host);
        }
        return hosts;
    }

}