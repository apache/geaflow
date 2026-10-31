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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.util.Collections;
import javax.servlet.Filter;
import javax.servlet.http.Cookie;
import org.apache.geaflow.kubernetes.operator.core.job.GeaflowJobApiService;
import org.apache.geaflow.kubernetes.operator.core.job.JobCRCache;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import org.apache.geaflow.kubernetes.operator.web.controller.HealthController;
import org.apache.geaflow.kubernetes.operator.web.controller.api.JobController;
import org.apache.geaflow.kubernetes.operator.web.mvc.ApiFilter;
import org.apache.geaflow.kubernetes.operator.web.mvc.WebConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = WebSecurityTest.TestConfiguration.class)
@TestPropertySource(properties = {
    "geaflow.operator.web.security.admin-token=" + WebSecurityTest.ADMIN,
    "geaflow.operator.web.security.viewer-token=" + WebSecurityTest.VIEWER
})
public class WebSecurityTest {

    static final String ADMIN = "0123456789abcdef0123456789abcdef";
    static final String VIEWER = "abcdef0123456789abcdef0123456789";
    private static final String JOB = "{\"name\":\"security-regression\",\"spec\":{"
        + "\"image\":\"example.invalid/trusted-job:1\",\"serviceAccount\":\"geaflow\","
        + "\"entryClass\":\"org.example.Job\"}}";

    @Autowired
    private WebApplicationContext context;

    @Autowired
    @Qualifier("springSecurityFilterChain")
    private Filter securityFilter;

    @Autowired
    private GeaflowJobApiService service;

    private MockMvc mvc;

    @Before
    public void setup() {
        reset(service);
        when(service.queryJobs()).thenReturn(Collections.emptyList());
        mvc = MockMvcBuilders.webAppContextSetup(context)
            .addFilters(securityFilter, new ApiFilter()).build();
    }

    @Test
    public void anonymousRequestsCannotReachBusinessServices() throws Exception {
        for (MockHttpServletRequestBuilder request : new MockHttpServletRequestBuilder[] {
            get("/api/jobs"), head("/api/jobs"), post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(JOB),
            post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content("{"),
            delete("/api/jobs/security-regression"), get("/api/overview"), get("/api/future"), get("/api/future.js"), options("/api/jobs")}) {
            mvc.perform(request).andExpect(status().isUnauthorized())
                .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, "Bearer realm=\"geaflow-operator\""))
                .andExpect(header().string(HttpHeaders.CACHE_CONTROL, "no-store"));
        }
        verifyNoInteractions(service);
    }

    @Test
    public void rejectsInvalidAndAmbiguousAuthorization() throws Exception {
        for (String value : new String[] {"", "Basic " + ADMIN, "Bearer", "Bearer bad", "Bearer " + ADMIN + " ",
                "Bearer " + ADMIN + ",Bearer " + VIEWER, "Bearer " + ADMIN + "x"}) {
            mvc.perform(get("/api/jobs").header(HttpHeaders.AUTHORIZATION, value))
                .andExpect(status().isUnauthorized());
        }
        mvc.perform(get("/api/jobs").header(HttpHeaders.AUTHORIZATION, "Bearer " + ADMIN, "Bearer " + VIEWER))
            .andExpect(status().isUnauthorized());
        verifyNoInteractions(service);
    }

    @Test
    public void ignoresQueryCookiesForwardedIdentityAndExistingSessions() throws Exception {
        MockHttpSession session = new MockHttpSession();
        session.setAttribute(HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY,
            new SecurityContextImpl(new OperatorTokens(ADMIN, VIEWER).authenticate(ADMIN)));
        mvc.perform(get("/api/jobs").param("access_token", ADMIN)
                .cookie(new Cookie("Authorization", "Bearer " + ADMIN))
                .header("X-Forwarded-User", "admin").header("X-Auth-Request-User", "admin").session(session))
            .andExpect(status().isUnauthorized());
        verifyNoInteractions(service);
    }

    @Test
    public void viewerCanReadButCannotCreateDeleteOrUseOtherMethods() throws Exception {
        mvc.perform(get("/api/jobs").header(HttpHeaders.AUTHORIZATION, "Bearer " + VIEWER))
            .andExpect(status().isOk());
        mvc.perform(head("/api/jobs").header(HttpHeaders.AUTHORIZATION, "Bearer " + VIEWER))
            .andExpect(status().isOk());
        reset(service);
        for (MockHttpServletRequestBuilder request : new MockHttpServletRequestBuilder[] {
            post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(JOB),
            delete("/api/jobs/security-regression"), options("/api/jobs"), post("/api/future")}) {
            mvc.perform(request.header(HttpHeaders.AUTHORIZATION, "Bearer " + VIEWER))
                .andExpect(status().isForbidden()).andExpect(content().json("{\"success\":false,\"message\":\"Access denied\"}"));
        }
        verifyNoInteractions(service);
    }

    @Test
    public void administratorCanSubmitAndDeleteJobsWithoutCreatingSession() throws Exception {
        mvc.perform(post("/api/jobs").header(HttpHeaders.AUTHORIZATION, "Bearer " + ADMIN)
                .contentType(MediaType.APPLICATION_JSON).content(JOB))
            .andExpect(status().isOk()).andExpect(result -> assertNull(result.getRequest().getSession(false)));
        ArgumentCaptor<GeaflowJob> job = ArgumentCaptor.forClass(GeaflowJob.class);
        verify(service).createJob(job.capture());
        assertEquals("example.invalid/trusted-job:1", job.getValue().getSpec().getImage());
        assertEquals("geaflow", job.getValue().getSpec().getServiceAccount());
        mvc.perform(delete("/api/jobs/security-regression").header(HttpHeaders.AUTHORIZATION, "Bearer " + ADMIN))
            .andExpect(status().isOk());
        verify(service).deleteJob("security-regression");
        mvc.perform(get("/api/jobs")).andExpect(status().isUnauthorized());
    }

    @Test
    public void acceptsCaseInsensitiveSchemeAndContextPath() throws Exception {
        mvc.perform(get("/operator/api/jobs").contextPath("/operator")
                .header(HttpHeaders.AUTHORIZATION, "bearer " + VIEWER))
            .andExpect(status().isOk());
    }

    @Test
    public void exposesHealthButDoesNotExemptOtherHealthPathsOrUnprefixedControllers() throws Exception {
        mvc.perform(get("/health")).andExpect(status().isOk());
        for (String path : new String[] {"/health/jobs", "/jobs", "/overview", "/api/jobs/", "/api/future"}) {
            mvc.perform(get(path)).andExpect(status().isUnauthorized());
        }
        mvc.perform(get("/jobs").header(HttpHeaders.AUTHORIZATION, "Bearer " + ADMIN))
            .andExpect(status().isForbidden());
        verifyNoInteractions(service);
    }

    @Test
    public void rejectsNonCanonicalPathsBeforeControllerDispatch() throws Exception {
        for (String path : new String[] {"/api//jobs", "/api/./jobs", "/api/jobs;x=1"}) {
            mvc.perform(get(URI.create(path)).header(HttpHeaders.AUTHORIZATION, "Bearer " + ADMIN))
                .andExpect(result -> assertEquals(path, 400, result.getResponse().getStatus()));
        }
        verifyNoInteractions(service);
    }

    @Test
    public void corsPreflightDoesNotGrantAnonymousAccess() throws Exception {
        mvc.perform(options("/api/jobs").header("Origin", "https://untrusted.invalid")
                .header("Access-Control-Request-Method", "POST")
                .header("Access-Control-Request-Headers", "authorization"))
            .andExpect(status().isUnauthorized()).andExpect(header().doesNotExist("Access-Control-Allow-Origin"));
        verifyNoInteractions(service);
    }

    @Configuration
    @EnableWebMvc
    @Import({WebSecurityConfiguration.class, WebConfig.class, JobController.class, HealthController.class})
    static class TestConfiguration {

        @Bean
        GeaflowJobApiService geaflowJobApiService() {
            return mock(GeaflowJobApiService.class);
        }

        @Bean
        JobCRCache jobCRCache() {
            return mock(JobCRCache.class);
        }
    }
}
