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

package org.apache.geaflow.console.biz.shared.convert;

import java.util.Optional;
import org.apache.geaflow.console.biz.shared.view.FunctionView;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FunctionViewConverter extends DataViewConverter<GeaflowFunction, FunctionView> {

    @Autowired
    private RemoteFileViewConverter remoteFileViewConverter;

    @Override
    public void merge(FunctionView view, FunctionView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getEntryClass()).ifPresent(view::setEntryClass);
    }

    @Override
    protected FunctionView modelToView(GeaflowFunction model) {
        FunctionView view = super.modelToView(model);
        view.setJarPackage(remoteFileViewConverter.convert(model.getJarPackage()));
        view.setEntryClass(model.getEntryClass());
        return view;
    }

    @Override
    protected GeaflowFunction viewToModel(FunctionView view) {
        GeaflowFunction geaflowFunction = super.viewToModel(view);
        geaflowFunction.setEntryClass(view.getEntryClass());
        return geaflowFunction;
    }

    public GeaflowFunction convert(FunctionView entity, GeaflowRemoteFile jarPackage) {
        GeaflowFunction model = viewToModel(entity);
        model.setJarPackage(jarPackage);
        return model;
    }
}
