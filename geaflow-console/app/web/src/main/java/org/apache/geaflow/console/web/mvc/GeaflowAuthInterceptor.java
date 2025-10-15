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

package org.apache.geaflow.console.web.mvc;

import com.google.common.base.Preconditions;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.biz.shared.AuthenticationManager;
import org.apache.geaflow.console.biz.shared.AuthorizationManager;
import org.apache.geaflow.console.biz.shared.TenantManager;
import org.apache.geaflow.console.biz.shared.view.AuthenticationView;
import org.apache.geaflow.console.biz.shared.view.TenantView;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.context.GeaflowContext;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.service.TaskService;
import org.apache.geaflow.console.core.service.security.TokenGenerator;
import org.apache.geaflow.console.web.api.GeaflowApiRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

@Component
@Slf4j
public class GeaflowAuthInterceptor implements HandlerInterceptor {

    @Autowired
    private TenantManager tenantManager;

    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @Autowired
    private TaskService taskService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
        throws Exception {
        if (request.getMethod().equals("OPTIONS")) {
            return true;
        }

        GeaflowContext context = ContextHolder.get();

        String token = GeaflowApiRequest.getSessionToken(request);

        if (TokenGenerator.isTaskToken(token)) {
            // The request is from tasks
            GeaflowId task = taskService.getByTaskToken(token);
            Preconditions.checkNotNull(task, "Invalid task token %s", token);
            context.setTaskId(task.getId());
            context.setUserId(task.getModifierId());
            context.setSystemSession(false);
            context.setSessionToken(token);
            context.setTenantId(task.getTenantId());
            context.getRoleTypes().addAll(authorizationManager.getUserRoleTypes(task.getModifierId()));
            return true;
        }

        AuthenticationView authentication = authenticationManager.authenticate(token);
        String userId = authentication.getUserId();
        boolean systemSession = authentication.isSystemSession();
        context.setUserId(userId);
        context.setSystemSession(systemSession);
        context.setSessionToken(token);
        if (!systemSession) {
            TenantView tenant = tenantManager.getActiveTenant(userId);
            context.setTenantId(tenant.getId());
        }

        context.getRoleTypes().addAll(authorizationManager.getUserRoleTypes(userId));
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
                           ModelAndView modelAndView) throws Exception {

    }
}
