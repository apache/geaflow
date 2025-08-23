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

package org.apache.geaflow.infer.exchange.serialize;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public class ByteArrayConstructor implements IObjectConstructor {

    private static final String LATIN = "latin-";

    private static final String ISO_KEY = "ISO-8859-";

    public Object construct(Object[] args) throws PickleException {
        if (args.length > 2) {
            throw new PickleException(
                "invalid pickle data for bytearray; expected 0, 1 or 2 args, got " + args.length);
        }

        if (args.length == 0) {
            return new byte[0];
        }

        if (args.length == 1) {
            if (args[0] instanceof byte[]) {
                return args[0];
            }

            ArrayList<Number> values = (ArrayList<Number>) args[0];
            byte[] data = new byte[values.size()];
            for (int i = 0; i < data.length; ++i) {
                data[i] = values.get(i).byteValue();
            }
            return data;
        } else {
            String data = (String) args[0];
            String encoding = (String) args[1];
            if (encoding.startsWith(LATIN)) {
                encoding = ISO_KEY + encoding.substring(6);
            }
            try {
                return data.getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                throw new PickleException("error creating bytearray: " + e);
            }
        }
    }
}
