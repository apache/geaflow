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

package org.apache.geaflow.console.biz.shared;

import java.util.List;
import org.apache.geaflow.console.biz.shared.view.IdView;
import org.apache.geaflow.console.common.dal.model.IdSearch;
import org.apache.geaflow.console.common.dal.model.PageList;

public interface IdManager<V extends IdView, S extends IdSearch> {

    PageList<V> search(S search);

    V get(String id);

    String create(V view);

    boolean updateById(String id, V view);

    boolean drop(String id);

    List<V> get(List<String> ids);

    List<String> create(List<V> views);

    boolean update(List<V> views);

    boolean drop(List<String> ids);
}
