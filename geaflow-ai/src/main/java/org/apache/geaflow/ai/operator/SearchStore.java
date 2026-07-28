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

import java.io.IOException;
import java.util.Map;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

/**
 * A lightweight in-memory Lucene index wrapper.
 *
 * <p>The store is designed to be <b>long lived</b>: writes are made visible to readers via
 * {@link #refresh()} (commit + near-real-time reader reopen) instead of closing the writer.
 * {@link #close()} is reserved for releasing the store for good.
 */
public class SearchStore {

    private final Directory directory = new ByteBuffersDirectory();
    private final Analyzer analyzer = new StandardAnalyzer();
    private final IndexWriterConfig config = new IndexWriterConfig(analyzer);
    private IndexWriter writer;
    private boolean writeStats = false;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private boolean readStats = false;
    private boolean pendingWrite = false;

    public SearchStore() {
    }

    public void addDoc(Map<String, String> kv) throws IOException {
        addDoc(kv, null);
    }

    /**
     * Adds a document. The field named {@code exactField}, if present in {@code kv}, is indexed as a
     * non analyzed {@link StringField} so that it can be used as a term for
     * {@link #updateDoc} and {@link #deleteDoc}.
     */
    public void addDoc(Map<String, String> kv, String exactField) throws IOException {
        initWriter();
        writer.addDocument(buildDoc(kv, exactField));
        pendingWrite = true;
    }

    /**
     * Replaces the document identified by {@code keyField = keyValue}, or adds it when absent.
     *
     * <p>Lucene implements this as「标记删除 + 新增」within one call, so the cost is proportional to
     * the change, not to the index size. Repeated calls with the same key are idempotent.
     */
    public void updateDoc(String keyField, String keyValue, Map<String, String> kv) throws IOException {
        initWriter();
        writer.updateDocument(new Term(keyField, keyValue), buildDoc(kv, keyField));
        pendingWrite = true;
    }

    /**
     * Marks the document identified by {@code keyField = keyValue} as deleted. Lucene only flips a
     * bit in a per segment bitset; space is reclaimed later by segment merging.
     */
    public void deleteDoc(String keyField, String keyValue) throws IOException {
        initWriter();
        writer.deleteDocuments(new Term(keyField, keyValue));
        pendingWrite = true;
    }

    private Document buildDoc(Map<String, String> kv, String exactField) {
        Document doc = new Document();
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            if (exactField != null && exactField.equals(entry.getKey())) {
                doc.add(new StringField(entry.getKey(), entry.getValue(), Field.Store.YES));
            } else {
                doc.add(new TextField(entry.getKey(), entry.getValue(), Field.Store.YES));
            }
        }
        return doc;
    }

    /**
     * Commits pending writes and reopens the reader so that newly added documents become
     * searchable. Safe to call repeatedly; it is a no-op when nothing changed.
     *
     * <p>This replaces the previous pattern of calling {@link #close()} before searching, which
     * forced the index to be discarded and rebuilt for every query.
     */
    public void refresh() throws IOException {
        if (writeStats && pendingWrite) {
            writer.commit();
            pendingWrite = false;
        }
        if (!readStats) {
            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);
            readStats = true;
            return;
        }
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        if (newReader != null) {
            reader.close();
            reader = newReader;
            searcher = new IndexSearcher(reader);
        }
    }

    public TopDocs searchDoc(String field, String content) throws ParseException, IOException {
        ensureSearcher();
        QueryParser parser = new QueryParser(field, analyzer);
        return searcher.search(parser.parse(content), Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN);
    }

    public Document getDoc(int docId) {
        try {
            ensureSearcher();
            return searcher.doc(docId);
        } catch (Throwable e) {
            return null;
        }
    }

    private void ensureSearcher() throws IOException {
        if (!readStats || pendingWrite) {
            refresh();
        }
    }

    public void initWriter() throws IOException {
        if (!writeStats) {
            writer = new IndexWriter(directory, config);
            writeStats = true;
        }
    }

    public void close() throws IOException {
        if (writeStats) {
            writer.close();
            writeStats = false;
            pendingWrite = false;
        }
        if (readStats) {
            reader.close();
            readStats = false;
            searcher = null;
        }
    }


    public Directory getDirectory() {
        return directory;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public IndexWriterConfig getConfig() {
        return config;
    }
}
