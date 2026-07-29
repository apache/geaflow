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

package org.apache.geaflow.dsl.connector.mongodb;

import org.apache.geaflow.dsl.connector.api.Offset;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

public class MongoOffset implements Offset {

    static final String ID_FIELD = "_id";
    static final String PARTITION_VALUE_FIELD = "partitionValue";

    private static final JsonWriterSettings JSON_SETTINGS = JsonWriterSettings.builder()
        .outputMode(JsonMode.EXTENDED)
        .build();

    private final long offset;
    private final String bookmark;

    public MongoOffset(long offset) {
        this(offset, null);
    }

    MongoOffset(long offset, BsonDocument bookmark) {
        if (offset < 0) {
            throw new IllegalArgumentException("MongoDB offset must not be negative");
        }
        this.offset = offset;
        this.bookmark = bookmark == null ? null : bookmark.toJson(JSON_SETTINGS);
    }

    boolean hasBookmark() {
        return bookmark != null;
    }

    BsonDocument getBookmark() {
        return bookmark == null ? null : BsonDocument.parse(bookmark);
    }

    @Override
    public String humanReadable() {
        return String.valueOf(offset);
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public boolean isTimestamp() {
        return false;
    }
}
