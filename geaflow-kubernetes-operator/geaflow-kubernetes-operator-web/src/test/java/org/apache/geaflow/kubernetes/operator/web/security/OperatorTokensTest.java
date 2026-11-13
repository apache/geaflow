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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import org.junit.Test;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.security.core.Authentication;

public class OperatorTokensTest {

    private static final String ADMIN = "0123456789abcdef0123456789abcdef";
    private static final String VIEWER = "abcdef0123456789abcdef0123456789";

    @Test
    public void rejectsMissingWeakMalformedAndIdenticalCredentials() {
        for (String invalid : Arrays.asList(null, "", " ", "short", ADMIN + " ", "Bearer " + ADMIN,
                ADMIN + "\n", "!" + ADMIN, "a".repeat(513))) {
            assertThrows(IllegalArgumentException.class, () -> new OperatorTokens(invalid, ""));
        }
        assertThrows(IllegalArgumentException.class, () -> new OperatorTokens(ADMIN, "short"));
        assertThrows(IllegalArgumentException.class, () -> new OperatorTokens(ADMIN, ADMIN));
    }

    @Test
    public void assignsSeparateRolesAndErasesCredentials() {
        OperatorTokens tokens = new OperatorTokens(ADMIN, VIEWER);
        Authentication admin = tokens.authenticate(ADMIN);
        assertEquals("ROLE_ADMIN", admin.getAuthorities().iterator().next().getAuthority());
        assertNull(admin.getCredentials());
        assertFalse(admin.toString().contains(ADMIN));
        assertEquals("ROLE_VIEWER", tokens.authenticate(VIEWER).getAuthorities().iterator().next().getAuthority());
        assertNull(tokens.authenticate(ADMIN + "x"));
        assertNull(tokens.authenticate(ADMIN.toUpperCase()));
        assertNull(tokens.authenticate(null));
        assertNull(tokens.authenticate("a".repeat(513)));
        assertNull(new OperatorTokens(ADMIN, "").authenticate(VIEWER));
    }

    @Test
    public void failsStartupWithoutValidAdministratorToken() {
        new WebApplicationContextRunner().withUserConfiguration(WebSecurityConfiguration.class)
            .run(context -> {
                assertThat(context).hasFailed();
                Throwable cause = context.getStartupFailure();
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                assertEquals("Configure geaflow.operator.web.security.admin-token"
                    + " with a random bearer token of 32 to 512 characters", cause.getMessage());
            });
    }
}
