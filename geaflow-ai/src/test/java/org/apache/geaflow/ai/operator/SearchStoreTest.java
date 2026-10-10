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

package org.apache.geaflow.ai.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import org.apache.lucene.search.TopDocs;
import org.junit.jupiter.api.Test;

class SearchStoreTest {

    @Test
    void searchDocHonorsRequestedTopN() throws Exception {
        SearchStore store = new SearchStore();
        try {
            store.addDoc(Collections.singletonMap(SearchConstants.CONTENT, "confucius teacher"));
            store.addDoc(Collections.singletonMap(SearchConstants.CONTENT, "confucius philosopher"));
            store.addDoc(Collections.singletonMap(SearchConstants.CONTENT, "confucius scholar"));
            store.finishWriting();

            TopDocs docs = store.searchDoc(SearchConstants.CONTENT, "confucius", 1);

            assertEquals(1, docs.scoreDocs.length);
        } finally {
            store.close();
            store.close();
        }
        assertThrows(IllegalStateException.class,
            () -> store.searchDoc(SearchConstants.CONTENT, "confucius", 1));
    }

    @Test
    void emptyAndWriteOnlyStoresAndGraphWrapperCanBeClosedRepeatedly() throws Exception {
        SearchStore empty = new SearchStore();
        empty.close();
        empty.close();
        SearchStore writeOnly = new SearchStore();
        writeOnly.addDoc(Collections.singletonMap(SearchConstants.CONTENT, "text"));
        writeOnly.close();
        writeOnly.close();
        assertThrows(IllegalStateException.class,
            () -> writeOnly.addDoc(Collections.singletonMap(SearchConstants.CONTENT, "text")));
        GraphSearchStore graph = new GraphSearchStore();
        graph.close();
        graph.close();
        assertThrows(IllegalStateException.class, graph::finishWriting);
    }
}
