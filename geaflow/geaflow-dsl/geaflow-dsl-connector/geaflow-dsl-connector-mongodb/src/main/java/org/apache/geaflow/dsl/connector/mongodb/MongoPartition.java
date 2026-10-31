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

import com.mongodb.client.model.Filters;
import java.util.Objects;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

public class MongoPartition implements Partition {

    private final String collection;
    private final int index;
    private final String field;
    private final Long lowerBound;
    private final Long upperBound;

    public MongoPartition(String collection, int index) {
        this(collection, index, null, null, null);
    }

    public MongoPartition(String collection, int index, String field, Long lowerBound,
                          Long upperBound) {
        this.collection = Objects.requireNonNull(collection);
        this.index = index;
        this.field = field;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public String getCollection() {
        return collection;
    }

    public String getField() {
        return field;
    }

    public Long getLowerBound() {
        return lowerBound;
    }

    public Long getUpperBound() {
        return upperBound;
    }

    public boolean hasRange() {
        return field != null;
    }

    public Bson toFilter() {
        if (!hasRange()) {
            return new BsonDocument();
        }
        return Filters.and(Filters.gte(field, lowerBound), Filters.lt(field, upperBound));
    }

    @Override
    public String getName() {
        return collection + "-" + index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongoPartition)) {
            return false;
        }
        MongoPartition that = (MongoPartition) o;
        return index == that.index && Objects.equals(collection, that.collection)
            && Objects.equals(field, that.field) && Objects.equals(lowerBound, that.lowerBound)
            && Objects.equals(upperBound, that.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(collection, index, field, lowerBound, upperBound);
    }
}
