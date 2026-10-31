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

package org.apache.geaflow.kubernetes.operator.web.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

/** Deployment-scoped credentials. Job submitters are trusted to execute code in the cluster. */
public final class OperatorTokens {

    private final byte[] adminDigest;
    private final byte[] viewerDigest;

    public OperatorTokens(String adminToken, String viewerToken) {
        validate(adminToken, "admin-token");
        adminDigest = digest(adminToken);
        if (viewerToken == null || viewerToken.isEmpty()) {
            viewerDigest = null;
        } else {
            validate(viewerToken, "viewer-token");
            viewerDigest = digest(viewerToken);
            if (MessageDigest.isEqual(adminDigest, viewerDigest)) {
                throw new IllegalArgumentException("Operator admin and viewer tokens must be different");
            }
        }
    }

    public Authentication authenticate(String token) {
        if (token == null || token.length() > 512) {
            return null;
        }
        byte[] candidate = digest(token);
        boolean admin = MessageDigest.isEqual(adminDigest, candidate);
        boolean viewer = viewerDigest != null && MessageDigest.isEqual(viewerDigest, candidate);
        if (admin) {
            return authentication("operator-admin", "ROLE_ADMIN");
        }
        if (viewer) {
            return authentication("operator-viewer", "ROLE_VIEWER");
        }
        return null;
    }

    private static Authentication authentication(String name, String role) {
        return new UsernamePasswordAuthenticationToken(name, null,
            Collections.singletonList(new SimpleGrantedAuthority(role)));
    }

    private static void validate(String token, String property) {
        // RFC 6750 b64token syntax. Require high-entropy, independently generated tokens.
        if (token == null || !token.matches("[A-Za-z0-9+/_~.\\-]{32,512}={0,2}") || token.length() > 512) {
            throw new IllegalArgumentException("Configure geaflow.operator.web.security." + property
                + " with a random bearer token of 32 to 512 characters");
        }
    }

    private static byte[] digest(String token) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
