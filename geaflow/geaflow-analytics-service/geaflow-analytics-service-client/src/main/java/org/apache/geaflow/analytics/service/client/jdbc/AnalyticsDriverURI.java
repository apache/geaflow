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

package org.apache.geaflow.analytics.service.client.jdbc;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.geaflow.analytics.service.client.jdbc.property.ConnectProperties.CUSTOM_HEADERS;
import static org.apache.geaflow.analytics.service.client.jdbc.property.ConnectProperties.SESSION_PROPERTIES;
import static org.apache.geaflow.analytics.service.client.utils.JDBCUtils.acceptsURL;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AnalyticsDriverURI {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsDriverURI.class);
    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);
    private static final String HTTP_PREFIX = "http";
    private static final String USER = "user";
    private static final String SECURE_HTTP_PREFIX = "https";
    private static final int JDBC_SCHEMA_OFFSET = 5;
    private static final int JDBC_SECURE_PORT = 443;
    private static final int MAX_PORT_LIMIT = 65535;
    private static final int MIN_PORT_LIMIT = 1;
    private static final String SYMBOL_SLASH = "/";
    private final URI uri;
    private final HostAndPort address;
    private final Properties properties;
    private final boolean useSecureConnection;
    private String graphView;
    private final String authority;

    public AnalyticsDriverURI(String url, Properties driverProperties) {
        this(parseDriverUrl(url), driverProperties);
    }

    private AnalyticsDriverURI(URI uri, Properties driverProperties) {
        this.uri = requireNonNull(uri, "analytics jdbc uri is null");
        this.address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        this.properties = mergeConnectionProperties(uri, driverProperties);
        this.useSecureConnection = uri.getPort() == JDBC_SECURE_PORT;
        this.authority = uri.getAuthority();
        initGraphView();
    }

    public String getGraphView() {
        return graphView;
    }

    public Properties getProperties() {
        return properties;
    }

    private static URI parseDriverUrl(String url) {
        URI jdbcUrl;
        try {
            jdbcUrl = acceptsURL(url) ? new URI(url.substring(JDBC_SCHEMA_OFFSET)) : null;
        } catch (URISyntaxException e) {
            String msg = String.format("illegal url: %s", url);
            throw new GeaflowRuntimeException(msg);
        }

        if (isNullOrEmpty(jdbcUrl.getHost())) {
            throw new GeaflowRuntimeException("No host specified: " + url);
        }

        if (jdbcUrl.getPort() == -1) {
            throw new GeaflowRuntimeException("No port number specified: " + url);
        }
        if ((jdbcUrl.getPort() < MIN_PORT_LIMIT) || (jdbcUrl.getPort() > MAX_PORT_LIMIT)) {
            throw new GeaflowRuntimeException("Invalid port number: " + url);
        }
        return jdbcUrl;
    }

    private static Properties mergeConnectionProperties(URI uri, Properties properties) {
        Map<String, String> urlProperties = parseParameters(uri.getQuery());
        Map<String, String> suppliedProperties = Maps.fromProperties(properties);

        for (String key : urlProperties.keySet()) {
            if (suppliedProperties.containsKey(key)) {
                throw new GeaflowRuntimeException(
                    String.format("Connection property '%s' is both in the URL and an "
                        + "argument", key));
            }
        }

        Properties result = new Properties();
        setProperties(result, urlProperties);
        setProperties(result, suppliedProperties);
        return result;
    }

    private void initGraphView() {
        String path = uri.getPath();
        if (isNullOrEmpty(uri.getPath()) || SYMBOL_SLASH.equals(path)) {
            return;
        }

        if (!path.startsWith(SYMBOL_SLASH)) {
            throw new GeaflowRuntimeException("Path does not start with a slash: " + uri);
        }

        path = path.substring(1);
        LOGGER.info("get server path {}, from url {}", path, uri);

        List<String> parts = Splitter.on(SYMBOL_SLASH).splitToList(path);
        if (parts.get(parts.size() - 1).isEmpty()) {
            parts = parts.subList(0, parts.size() - 1);
        }
        if (parts.size() > 2) {
            throw new GeaflowRuntimeException("Invalid path segments in URL: " + uri);
        }

        if (parts.get(0).isEmpty()) {
            throw new GeaflowRuntimeException("Graph view is empty: " + uri);
        }
        graphView = parts.get(0);
    }

    private static void setProperties(Properties properties, Map<String, String> values) {
        for (Entry<String, String> entry : values.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
    }

    public String getUser() {
        String user = properties.getProperty(USER);
        if (StringUtils.isEmpty(user)) {
            throw new GeaflowRuntimeException("connect user is null");
        }
        return user;
    }

    public boolean isCompressionDisabled() {
        return false;
    }

    public URI getHttpUri() {
        String scheme = useSecureConnection ? SECURE_HTTP_PREFIX : HTTP_PREFIX;
        try {
            return new URI(scheme, null, address.getHost(), address.getPort(), null, null, null);
        } catch (URISyntaxException e) {
            throw new GeaflowRuntimeException("get http uri fail", e);
        }
    }

    public Map<String, String> getSessionProperties() {
        return SESSION_PROPERTIES.getValue(properties).orElse(ImmutableMap.of());
    }

    public Map<String, String> getCustomHeaders() {
        return CUSTOM_HEADERS.getValue(properties).orElse(ImmutableMap.of());
    }

    private static Map<String, String> parseParameters(String query) {
        Map<String, String> result = new HashMap<>();
        if (query != null) {
            Iterable<String> queryArgs = QUERY_SPLITTER.split(query);
            for (String queryArg : queryArgs) {
                List<String> parts = ARG_SPLITTER.splitToList(queryArg);
                if (result.put(parts.get(0), parts.get(1)) != null) {
                    throw new GeaflowRuntimeException(format("Connection property '%s' is in URL multiple times", parts.get(0)));
                }
            }
        }
        return result;
    }

    public String getAuthority() {
        return authority;
    }
}
