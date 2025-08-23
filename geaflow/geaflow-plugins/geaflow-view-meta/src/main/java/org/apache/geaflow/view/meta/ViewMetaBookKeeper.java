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

package org.apache.geaflow.view.meta;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.utils.FileUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentIOBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewMetaBookKeeper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewMetaBookKeeper.class);
    private static final String VIEW_VERSION = "view_version";
    private final String viewName;
    private final ViewMetaKeeper viewMetaKeeper;

    public ViewMetaBookKeeper(String myViewName, Configuration config) {
        this.viewName = myViewName;
        viewMetaKeeper = new ViewMetaKeeper();
        viewMetaKeeper.init(myViewName, config);
    }

    public long getLatestViewVersion(String name) throws IOException {
        byte[] res = viewMetaKeeper.get(name, VIEW_VERSION);
        return res == null ? -1L : Longs.fromByteArray(res);
    }

    public void saveViewVersion(long viewVersion) throws IOException {
        viewMetaKeeper.save(VIEW_VERSION, Longs.toByteArray(viewVersion));
        LOGGER.info("save view version {} {}", viewName, viewVersion);
    }

    public void archive() {
        try {
            viewMetaKeeper.archive();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getViewName() {
        return viewName;
    }

    public static class ViewMetaKeeper {

        private static final String INFO_FILE_NAME = "view.meta";
        // file modify time unit is second.
        private static final long SHARED_STATE_INFO_REFRESH_MS_THRESHOLD = 1000;
        private IPersistentIO persistIO;
        private Map<String, Tuple<ViewMeta, Long>> sharedViewMeta = new HashMap<>();
        private ViewMeta myStateInfo;
        private String myViewName;
        private String namespace;

        public void init(String viewName, Configuration config) {
            this.myViewName = viewName;
            this.persistIO = PersistentIOBuilder.build(config);
            this.namespace = config.getString(FileConfigKeys.ROOT);
        }

        private ViewMeta getOrInit(String viewName) throws IOException {
            boolean isMyself = myViewName.equals(viewName);
            if (!isMyself) {
                Tuple<ViewMeta, Long> tuple = sharedViewMeta.get(viewName);
                ViewMeta viewMeta;
                if (tuple == null) {
                    String file = FileUtil.concatPath(this.namespace, viewName) + "/" + INFO_FILE_NAME;
                    viewMeta = new ViewMeta(file, persistIO);
                    this.sharedViewMeta.put(viewName, Tuple.of(viewMeta, System.currentTimeMillis()));
                } else if (System.currentTimeMillis() - tuple.f1 > SHARED_STATE_INFO_REFRESH_MS_THRESHOLD) {
                    tryRefresh(tuple);
                    viewMeta = tuple.f0;
                } else {
                    viewMeta = tuple.f0;
                }
                return viewMeta;
            } else {
                if (myStateInfo == null) {
                    String file = FileUtil.concatPath(this.namespace, viewName) + "/" + INFO_FILE_NAME;
                    myStateInfo = new ViewMeta(file, persistIO);
                }
                return myStateInfo;
            }
        }

        private void tryRefresh(Tuple<ViewMeta, Long> tuple) throws IOException {
            tuple.f0.tryRefresh();
            tuple.f1 = System.currentTimeMillis();
        }

        public void save(String k, byte[] v) throws IOException {
            Preconditions.checkNotNull(k);
            Preconditions.checkNotNull(v);
            ViewMeta stateInfo = getOrInit(myViewName);
            stateInfo.getKVMap().put(k, ByteString.copyFrom(v));
        }

        public byte[] get(String k) throws IOException {
            return get(myViewName, k);
        }

        public byte[] get(String viewName, String k) throws IOException {
            ViewMeta viewMeta = getOrInit(viewName);
            Preconditions.checkNotNull(viewMeta);
            ByteString value = viewMeta.getKVMap().get(k);
            if (value == null) {
                return null;
            }
            return value.toByteArray();
        }

        public void archive() throws IOException {
            long t = System.currentTimeMillis();
            myStateInfo.archive();
            LOGGER.info("archive view meta cost {}ms", System.currentTimeMillis() - t);
        }
    }
}
