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

package org.apache.geaflow.console.core.model.security;

import java.util.LinkedHashSet;
import java.util.Set;
import lombok.Getter;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.common.util.type.GeaflowRoleType;
import org.apache.geaflow.console.core.model.security.resource.AllResource;
import org.apache.geaflow.console.core.model.security.resource.TenantResource;

@Getter
public class GeaflowRole {

    public static final GeaflowRole SYSTEM_ADMIN = new GeaflowRole(GeaflowRoleType.SYSTEM_ADMIN, null);

    public static final GeaflowRole TENANT_ADMIN = new GeaflowRole(GeaflowRoleType.TENANT_ADMIN, SYSTEM_ADMIN);

    private final GeaflowRoleType type;

    private final GeaflowRole parent;

    private GeaflowRole(GeaflowRoleType type, GeaflowRole parent) {
        this.type = type;
        this.parent = parent;
    }

    public static GeaflowRole of(GeaflowRoleType type) {
        switch (type) {
            case SYSTEM_ADMIN:
                return SYSTEM_ADMIN;
            case TENANT_ADMIN:
                return TENANT_ADMIN;
            default:
                throw new GeaflowException("Role type {} not supported", type);
        }
    }

    public static Set<GeaflowGrant> getGrants(Set<GeaflowRoleType> roleTypes) {
        Set<GeaflowGrant> grants = new LinkedHashSet<>();
        for (GeaflowRoleType roleType : roleTypes) {
            GeaflowRole role = GeaflowRole.of(roleType);
            grants.addAll(role.getGrants());
        }
        return grants;
    }

    public Set<GeaflowGrant> getGrants() {
        Set<GeaflowGrant> grants = new LinkedHashSet<>();
        switch (type) {
            case SYSTEM_ADMIN:
                grants.add(new GeaflowGrant(GeaflowAuthority.ALL, new AllResource(GeaflowResourceType.TENANT)));
                break;
            case TENANT_ADMIN:
                String tenantId = ContextHolder.get().getTenantId();
                grants.add(new GeaflowGrant(GeaflowAuthority.ALL, new TenantResource(tenantId)));
                break;
            default:
                throw new GeaflowException("Role type {} not supported", type);
        }
        return grants;
    }
}
