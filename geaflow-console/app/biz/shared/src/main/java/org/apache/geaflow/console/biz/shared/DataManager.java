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

public interface DataManager<V extends IdView, S extends IdSearch> extends NameManager<V, S> {

    V getByName(String instanceName, String name);

    List<V> getByNames(String instanceName, List<String> names);

    boolean dropByName(String instanceName, String name);

    boolean dropByNames(String instanceName, List<String> names);

    List<String> create(String instanceName, List<V> views);

    String create(String instanceName, V view);

    boolean update(String instanceName, List<V> views);

    boolean updateByName(String instanceName, String name, V view);

    PageList<V> searchByInstanceName(String instanceName, S search);

    void createIfIdAbsent(String instanceName, List<V> views);
}
