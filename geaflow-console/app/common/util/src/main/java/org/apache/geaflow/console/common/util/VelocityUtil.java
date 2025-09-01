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

package org.apache.geaflow.console.common.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

public class VelocityUtil {

    private static VelocityEngine VELOCITY_ENGINE = null;

    private static final ConcurrentHashMap<String, Template> TEMPLATES = new ConcurrentHashMap<>();

    private static final String END_FLAG = "END_FLAG";

    private static void initEngineIfNeeded() {
        if (VELOCITY_ENGINE == null) {
            synchronized (VelocityUtil.class) {
                try {

                    VelocityEngine engine = new VelocityEngine();
                    engine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
                    engine.setProperty("classpath.resource.loader.class", FormatResourceLoader.class.getName());

                    engine.init();
                    VELOCITY_ENGINE = engine;

                } catch (Exception e) {
                    throw new RuntimeException("Init Velocity Engine Failed", e);
                }
            }
        }
    }

    public static String applyResource(String resourceName, Map<String, Object> params) {
        if (StringUtils.isBlank(resourceName)) {
            throw new RuntimeException("Invalid Resource Name");
        }

        if (params == null) {
            params = new HashMap<>();
        }

        params.put(END_FLAG, END_FLAG);
        initEngineIfNeeded();

        try {
            Template template = TEMPLATES.computeIfAbsent(resourceName, s -> {
                try {
                    return VELOCITY_ENGINE.getTemplate(s, "utf-8");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            VelocityContext context = new VelocityContext();
            params.forEach(context::put);

            StringWriter writer = new StringWriter();
            template.merge(context, writer);

            return StringUtils.replacePattern(writer.toString(), ",\n*\\s*" + END_FLAG, "");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class FormatResourceLoader extends ClasspathResourceLoader {

        public static class VTLIndentationGlobber extends FilterInputStream {

            protected String buffer = "";
            protected int bufpos = 0;

            protected enum State {
                defstate, hash, comment, directive, schmoo, eol, eof
            }

            protected State state =
                State.defstate;

            public VTLIndentationGlobber(InputStream is) {
                super(is);
            }

            public int read() throws IOException {
                while (true) {
                    switch (state) {
                        case defstate: {
                            int ch = in.read();
                            switch (ch) {
                                case (int) '#':
                                    state = State.hash;
                                    buffer = "";
                                    bufpos = 0;
                                    return ch;
                                case (int) ' ':
                                case (int) '\t':
                                    buffer += (char) ' ';
                                    break;
                                case -1:
                                    state = State.eof;
                                    break;
                                default:
                                    buffer += (char) ch;
                                    state = State.schmoo;
                                    break;
                            }
                            break;
                        }
                        case eol:
                            if (bufpos < buffer.length()) {
                                return (int) buffer.charAt(bufpos++);
                            } else {
                                state = State.defstate;
                                buffer = "";
                                bufpos = 0;
                                return '\n';
                            }
                        case eof:
                            if (bufpos < buffer.length()) {
                                return (int) buffer.charAt(bufpos++);
                            } else {
                                return -1;
                            }
                        case hash: {
                            int ch = (int) in.read();
                            switch (ch) {
                                case (int) '#':
                                    state = State.directive;
                                    return ch;
                                case -1:
                                    state = State.eof;
                                    return -1;
                                default:
                                    state = State.directive;
                                    buffer = "##";
                                    return ch;
                            }
                        }
                        case directive: {
                            int ch = (int) in.read();
                            if (ch == (int) '\n') {
                                state = State.eol;
                                break;
                            } else if (ch == -1) {
                                state = State.eof;
                                break;
                            } else {
                                return ch;
                            }
                        }
                        case schmoo: {
                            int ch = (int) in.read();
                            if (ch == (int) '\n') {
                                state = State.eol;
                                break;
                            } else if (ch == -1) {
                                state = State.eof;
                                break;
                            } else {
                                buffer += (char) ch;
                                return (int) buffer.charAt(bufpos++);
                            }
                        }
                        default:
                            break;
                    }
                }
            }

            public int read(byte[] b, int off, int len) throws IOException {
                int i;
                int ok = 0;
                while (len-- > 0) {
                    i = read();
                    if (i == -1) {
                        return (ok == 0) ? -1 : ok;
                    }
                    b[off++] = (byte) i;
                    ok++;
                }
                return ok;
            }

            public int read(byte[] b) throws IOException {
                return read(b, 0, b.length);
            }

            public boolean markSupported() {
                return false;
            }
        }

        @Override
        public synchronized InputStream getResourceStream(String name) {
            return new VTLIndentationGlobber(super.getResourceStream(name));
        }
    }
}
