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

package org.apache.geaflow.console.web.aspect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.console.biz.shared.TenantManager;
import org.apache.geaflow.console.biz.shared.UserManager;
import org.apache.geaflow.console.biz.shared.view.IdView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.web.api.SuccessApiResponse;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ViewNameAspect {

    @Autowired
    private UserManager userManager;

    @Autowired
    private TenantManager tenantManager;

    @AfterReturning(value = "execution(* org.apache.geaflow.console.web.controller..*Controller.*(..))", returning
        = "response")
    public void handle(JoinPoint joinPoint, Object response) throws Throwable {
        if (response instanceof SuccessApiResponse) {
            Object data = ((SuccessApiResponse<?>) response).getData();

            if (data instanceof IdView) {
                fillName(Collections.singleton(data));

            } else if (data instanceof Collection) {
                fillName((Collection<?>) data);

            } else if (data instanceof Map) {
                fillName(((Map<?, ?>) data).values());

            } else if (data instanceof PageList) {
                fillName(((PageList<?>) data).getList());
            }
        }
    }

    private void fillName(Collection<?> collection) {
        fillUserName(collection);
        fillTenantName(collection);
    }

    private void fillUserName(Collection<?> collection) {
        List<IdView> creatorViews = new ArrayList<>();
        List<IdView> modifierViews = new ArrayList<>();
        Set<String> userIds = new HashSet<>();

        collection.forEach(v -> {
            if (v instanceof IdView) {
                IdView idView = (IdView) v;
                String creatorId = idView.getCreatorId();
                String modifierId = idView.getModifierId();

                if (creatorId != null) {
                    userIds.add(creatorId);
                    creatorViews.add(idView);
                }
                if (modifierId != null) {
                    userIds.add(modifierId);
                    modifierViews.add(idView);
                }
            }
        });

        if (!userIds.isEmpty()) {
            Map<String, String> userNames = userManager.getUserNames(userIds);
            creatorViews.forEach(v -> v.setCreatorName(userNames.get(v.getCreatorId())));
            modifierViews.forEach(v -> v.setModifierName(userNames.get(v.getModifierId())));
        }
    }

    private void fillTenantName(Collection<?> collection) {
        List<IdView> views = new ArrayList<>();
        Set<String> tenantIds = new HashSet<>();

        collection.forEach(v -> {
            if (v instanceof IdView) {
                IdView idView = (IdView) v;
                String tenantId = idView.getTenantId();

                if (tenantId != null) {
                    tenantIds.add(tenantId);
                    views.add(idView);
                }
            }
        });

        if (!tenantIds.isEmpty()) {
            Map<String, String> tenantNames = tenantManager.getTenantNames(tenantIds);
            views.forEach(v -> v.setTenantName(tenantNames.get(v.getTenantId())));
        }
    }
}
