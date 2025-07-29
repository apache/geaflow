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

package org.apache.geaflow.model.graph.meta;

import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;

@SuppressWarnings("rawtypes")
public class GraphFieldSerializers {

    public static class IdFieldSerializer<ELEMENT> implements IGraphFieldSerializer<ELEMENT> {

        public static final IdFieldSerializer INSTANCE = new IdFieldSerializer();

        @Override
        public Object getValue(ELEMENT element, GraphFiledName field) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.typeSysError(keyTypeErrMsg(element.getClass(), field.name())));
        }

        @Override
        public void setValue(ELEMENT element, GraphFiledName field, Object value) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.typeSysError(keyTypeErrMsg(element.getClass(), field.name())));
        }

    }

    public static class LabelFieldSerializer<ELEMENT extends IGraphElementWithLabelField> implements IGraphFieldSerializer<ELEMENT> {

        public static final LabelFieldSerializer INSTANCE = new LabelFieldSerializer();

        @Override
        public Object getValue(ELEMENT element, GraphFiledName field) {
            if (field == GraphFiledName.LABEL) {
                return element.getLabel();
            } else {
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.typeSysError(keyTypeErrMsg(element.getClass(), field.name())));
            }
        }

        @Override
        public void setValue(ELEMENT element, GraphFiledName field, Object value) {
            if (field == GraphFiledName.LABEL) {
                element.setLabel((String) value);
            } else {
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.typeSysError(keyTypeErrMsg(element.getClass(), field.name())));
            }
        }

    }

    public static class TimeFieldSerializer<ELEMENT extends IGraphElementWithTimeField> implements IGraphFieldSerializer<ELEMENT> {

        public static final TimeFieldSerializer INSTANCE = new TimeFieldSerializer();

        @Override
        public Object getValue(ELEMENT element, GraphFiledName field) {
            if (field == GraphFiledName.TIME) {
                return element.getTime();
            } else {
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.typeSysError(keyTypeErrMsg(element.getClass(), field.name())));
            }
        }

        @Override
        public void setValue(ELEMENT vertex, GraphFiledName field, Object value) {
            if (field == GraphFiledName.TIME) {
                vertex.setTime((long) value);
            } else {
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.typeSysError(keyTypeErrMsg(vertex.getClass(), field.name())));
            }
        }

    }

    public static class LabelTimeFieldSerializer<ELEMENT extends IGraphElementWithLabelField & IGraphElementWithTimeField>
        implements IGraphFieldSerializer<ELEMENT> {

        public static final LabelTimeFieldSerializer INSTANCE = new LabelTimeFieldSerializer();

        @Override
        public Object getValue(ELEMENT element, GraphFiledName field) {
            switch (field) {
                case LABEL:
                    return element.getLabel();
                case TIME:
                    return element.getTime();
                default:
                    throw new GeaflowRuntimeException(
                        RuntimeErrors.INST.typeSysError(keyTypeErrMsg(element.getClass(), field.name())));
            }
        }

        @Override
        public void setValue(ELEMENT element, GraphFiledName field, Object value) {
            switch (field) {
                case LABEL:
                    element.setLabel((String) value);
                    return;
                case TIME:
                    element.setTime((long) value);
                    return;
                default:
                    throw new GeaflowRuntimeException(
                        RuntimeErrors.INST.typeSysError(keyTypeErrMsg(element.getClass(), field.name())));
            }
        }

    }

    private static String keyTypeErrMsg(Class<?> clazz, String key) {
        return String.format("unrecognized key [%s] of [%s]", key, clazz.getCanonicalName());
    }

}
