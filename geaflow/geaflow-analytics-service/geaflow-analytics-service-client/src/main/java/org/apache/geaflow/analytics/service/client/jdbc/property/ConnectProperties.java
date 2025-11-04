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

package org.apache.geaflow.analytics.service.client.jdbc.property;

import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.geaflow.analytics.service.client.jdbc.property.AbstractConnectProperty.StringMapConverter.STRING_MAP_CONVERTER;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;

/* This file is based on source code from the Presto Project (https://prestodb.github.io/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Presto's com.facebook.presto.jdbc.ConnectionProperties.
 */
public class ConnectProperties {

    public static final ConnectProperty<String> USER = new User();
    public static final ConnectProperty<String> PASSWORD = new Password();
    public static final ConnectProperty<HostAndPort> SOCKS_PROXY = new SocksProxy();
    public static final ConnectProperty<HostAndPort> HTTP_PROXY = new HttpProxy();
    public static final ConnectProperty<Boolean> DISABLE_COMPRESSION = new DisableCompression();
    public static final ConnectProperty<String> ACCESS_TOKEN = new AccessToken();
    public static final ConnectProperty<Map<String, String>> CUSTOM_HEADERS = new CustomHeaders();
    public static final ConnectProperty<Map<String, String>> SESSION_PROPERTIES = new SessionProperties();

    private static final Set<ConnectProperty<?>> ALL_PROPERTIES =
        ImmutableSet.<ConnectProperty<?>>builder()
            .add(USER)
            .add(PASSWORD)
            .add(SOCKS_PROXY)
            .add(HTTP_PROXY)
            .add(DISABLE_COMPRESSION)
            .add(ACCESS_TOKEN)
            .add(CUSTOM_HEADERS)
            .add(SESSION_PROPERTIES)
            .build();

    private static final Map<String, ConnectProperty<?>> KEY_LOOKUP = unmodifiableMap(
        ALL_PROPERTIES.stream()
            .collect(toMap(ConnectProperty::getPropertyKey, identity())));

    private static final Map<String, String> DEFAULTS;

    static {
        ImmutableMap.Builder<String, String> defaults = ImmutableMap.builder();
        for (ConnectProperty<?> property : ALL_PROPERTIES) {
            property.getDefault()
                .ifPresent(value -> defaults.put(property.getPropertyKey(), value));
        }
        DEFAULTS = defaults.build();
    }

    private ConnectProperties() {
    }

    public static ConnectProperty<?> forKey(String propertiesKey) {
        return KEY_LOOKUP.get(propertiesKey);
    }

    public static Set<ConnectProperty<?>> allProperties() {
        return ALL_PROPERTIES;
    }

    public static Map<String, String> getDefaults() {
        return DEFAULTS;
    }

    private static class User
        extends AbstractConnectProperty<String> {

        public User() {
            super(User.class.getSimpleName().toLowerCase(), REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Password
        extends AbstractConnectProperty<String> {

        public Password() {
            super(Password.class.getSimpleName().toLowerCase(), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class SocksProxy
        extends AbstractConnectProperty<HostAndPort> {

        private static final Predicate<Properties> NO_HTTP_PROXY =
            checkedPredicate(properties -> !HTTP_PROXY.getValue(properties).isPresent());

        public SocksProxy() {
            super(SocksProxy.class.getSimpleName().toLowerCase(), NOT_REQUIRED, NO_HTTP_PROXY, HostAndPort::fromString);
        }
    }

    private static class HttpProxy
        extends AbstractConnectProperty<HostAndPort> {

        private static final Predicate<Properties> NO_SOCKS_PROXY =
            checkedPredicate(properties -> !SOCKS_PROXY.getValue(properties).isPresent());

        public HttpProxy() {
            super(HttpProxy.class.getSimpleName().toLowerCase(), NOT_REQUIRED, NO_SOCKS_PROXY,
                HostAndPort::fromString);
        }
    }


    private static class DisableCompression
        extends AbstractConnectProperty<Boolean> {

        public DisableCompression() {
            super(DisableCompression.class.getSimpleName().toLowerCase(), NOT_REQUIRED, ALLOWED,
                BOOLEAN_CONVERTER);
        }
    }

    private static class AccessToken
        extends AbstractConnectProperty<String> {

        public AccessToken() {
            super(AccessToken.class.getSimpleName().toLowerCase(), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class CustomHeaders
        extends AbstractConnectProperty<Map<String, String>> {

        public CustomHeaders() {
            super(CustomHeaders.class.getSimpleName().toLowerCase(), NOT_REQUIRED, ALLOWED, STRING_MAP_CONVERTER);
        }
    }

    private static class SessionProperties
        extends AbstractConnectProperty<Map<String, String>> {

        public SessionProperties() {
            super(SessionProperties.class.getSimpleName().toLowerCase(), NOT_REQUIRED, ALLOWED, STRING_MAP_CONVERTER);
        }
    }

}
