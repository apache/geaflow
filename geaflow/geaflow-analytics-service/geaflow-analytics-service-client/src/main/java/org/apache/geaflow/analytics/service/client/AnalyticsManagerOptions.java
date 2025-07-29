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

package org.apache.geaflow.analytics.service.client;

import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;

import com.google.common.net.HostAndPort;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class AnalyticsManagerOptions {

    private static final int DEFAULT_PORT = 8080;

    private static final int DEFAULT_REQUEST_TIMEOUT_SECOND = 120;

    private static final String HTTP_PREFIX = "http://";

    private static final String DELIMITER = ":";

    private static final String PARSER_PREFIX = "http";

    private static final String HTTPS_PREFIX = "https://";

    private static final String DEFAULT_SERVER = "localhost";

    private static final String DEFAULT_USER = System.getProperty("user.name");

    private static final String DEFAULT_SOURCE = "geaflow-client";

    public static AnalyticsManagerSession createClientSession(String host, int port) {
        String url = HTTP_PREFIX + host + DELIMITER + port;
        return new AnalyticsManagerSession(parseServer(url), DEFAULT_USER, DEFAULT_SOURCE,
            DEFAULT_REQUEST_TIMEOUT_SECOND, false, emptyMap(), emptyMap());
    }

    public static AnalyticsManagerSession createClientSession(int port) {
        return new AnalyticsManagerSession(parseServer(DEFAULT_SERVER, port), DEFAULT_USER, DEFAULT_SOURCE,
            DEFAULT_REQUEST_TIMEOUT_SECOND, false, emptyMap(), emptyMap());
    }

    public static URI parseServer(String server) {
        server = server.toLowerCase(ENGLISH);
        if (server.startsWith(HTTP_PREFIX) || server.startsWith(HTTPS_PREFIX)) {
            return URI.create(server);
        }
        HostAndPort host = HostAndPort.fromString(server);
        try {
            return new URI(PARSER_PREFIX, null, host.getHost(), host.getPortOrDefault(DEFAULT_PORT),
                null, null, null);
        } catch (URISyntaxException e) {
            throw new GeaflowRuntimeException("parse http server error", e);
        }
    }

    public static URI parseServer(String server, int port) {
        server = server.toLowerCase(ENGLISH);
        if (server.startsWith(HTTP_PREFIX) || server.startsWith(HTTPS_PREFIX)) {
            return URI.create(server);
        }
        HostAndPort host = HostAndPort.fromString(server);
        try {
            return new URI(PARSER_PREFIX, null, host.getHost(), port,
                null, null, null);
        } catch (URISyntaxException e) {
            throw new GeaflowRuntimeException("parse http server error", e);
        }
    }

}
