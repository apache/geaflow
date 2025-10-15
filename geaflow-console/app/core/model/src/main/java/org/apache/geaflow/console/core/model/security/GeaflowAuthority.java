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

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowAuthorityType;

@Getter
@AllArgsConstructor
public class GeaflowAuthority {

    public static final GeaflowAuthority ALL = new GeaflowAuthority(GeaflowAuthorityType.ALL);
    public static final GeaflowAuthority QUERY = new GeaflowAuthority(GeaflowAuthorityType.QUERY);
    public static final GeaflowAuthority UPDATE = new GeaflowAuthority(GeaflowAuthorityType.UPDATE);
    public static final GeaflowAuthority EXECUTE = new GeaflowAuthority(GeaflowAuthorityType.EXECUTE);
    private static final Map<GeaflowAuthorityType, GeaflowAuthority> AUTHORITIES = new HashMap<>();

    static {
        register(new GeaflowAuthority(GeaflowAuthorityType.ALL));
        register(new GeaflowAuthority(GeaflowAuthorityType.QUERY));
        register(new GeaflowAuthority(GeaflowAuthorityType.UPDATE));
        register(new GeaflowAuthority(GeaflowAuthorityType.EXECUTE));
    }

    private GeaflowAuthorityType type;

    public static GeaflowAuthority of(GeaflowAuthorityType type) {
        GeaflowAuthority authority = AUTHORITIES.get(type);
        if (authority == null) {
            throw new GeaflowException("Authority type {} not supported", type);
        }
        return authority;
    }

    private static void register(GeaflowAuthority authority) {
        AUTHORITIES.put(authority.type, authority);
    }

    public boolean include(GeaflowAuthority other) {
        if (other == null) {
            return false;
        }

        return other.getType() == type || GeaflowAuthorityType.ALL.equals(type);
    }
}
