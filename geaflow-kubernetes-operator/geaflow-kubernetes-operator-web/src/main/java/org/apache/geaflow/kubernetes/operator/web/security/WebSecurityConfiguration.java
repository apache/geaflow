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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

@Configuration
@EnableWebSecurity
public class WebSecurityConfiguration {

    @Bean
    public OperatorTokens operatorTokens(
            @Value("${geaflow.operator.web.security.admin-token:}") String adminToken,
            @Value("${geaflow.operator.web.security.viewer-token:}") String viewerToken) {
        return new OperatorTokens(adminToken, viewerToken);
    }

    @Bean
    public WebSecurityCustomizer operatorFirewallErrors() {
        // Keep StrictHttpFirewall's path checks and return a generic response without an error dispatch.
        return web -> web.requestRejectedHandler((request, response, error) -> SecurityResponse.badRequest(response));
    }

    @Bean
    public AuthenticationManager operatorAuthenticationManager() {
        // Disable fallback accounts, including Spring Boot's generated default user.
        return authentication -> {
            throw new BadCredentialsException("Bearer token authentication required");
        };
    }

    @Bean
    public SecurityFilterChain operatorSecurityFilterChain(HttpSecurity http, OperatorTokens tokens,
                                                          AuthenticationManager authenticationManager) throws Exception {
        // Only explicit Authorization headers authenticate requests. No cookies, Basic auth,
        // form login or sessions are accepted, so browsers cannot send credentials implicitly.
        http.authenticationManager(authenticationManager)
            .csrf().disable()
            .cors().disable()
            .httpBasic().disable()
            .formLogin().disable()
            .logout().disable()
            .requestCache().disable()
            .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            .and().exceptionHandling()
            .authenticationEntryPoint((request, response, error) -> SecurityResponse.unauthorized(response))
            .accessDeniedHandler((request, response, error) -> SecurityResponse.forbidden(response))
            .and().authorizeRequests()
            .antMatchers(HttpMethod.GET, "/api", "/api/**").hasAnyRole("ADMIN", "VIEWER")
            .antMatchers(HttpMethod.HEAD, "/api", "/api/**").hasAnyRole("ADMIN", "VIEWER")
            .antMatchers("/api", "/api/**").hasRole("ADMIN")
            .antMatchers(HttpMethod.GET, "/health", "/", "/index.html", "/*.ico", "/*.svg",
                "/*.js", "/*.css", "/*.png", "/*.jpg", "/*.jpeg", "/*.gif", "/*.webp",
                "/*.woff", "/*.woff2", "/*.ttf", "/*.eot", "/scripts/**", "/static/**", "/assets/**", "/icons/**").permitAll()
            .anyRequest().denyAll()
            .and().addFilterBefore(new BearerTokenFilter(tokens), AnonymousAuthenticationFilter.class);
        return http.build();
    }
}
