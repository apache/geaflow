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
 * {@link #refresh()}, a near-real-time reader reopen, instead of closing the writer.
 * {@link #close()} is reserved for releasing the store for good.
 *
 * <p>Reader state is volatile so that searches may run concurrently with each other. Writes and
 * {@link #refresh()} are not thread safe and must be serialized by the caller.
 */
public class SearchStore {

    private final Directory directory = new ByteBuffersDirectory();
    private final Analyzer analyzer = new StandardAnalyzer();
    private IndexWriter writer;
    private volatile boolean writeStats = false;
    private volatile DirectoryReader reader;
    private volatile IndexSearcher searcher;
    private volatile boolean readStats = false;
    private volatile boolean pendingWrite = false;
    /**
     * Whether {@link #reader} was opened from the writer, and can therefore be reopened from it.
     */
    private volatile boolean nearRealTimeReader = false;

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
     * <p>Lucene implements this as a tombstone plus an insert within one call, so the cost is
     * proportional to the change, not to the index size. Repeated calls with the same key are
     * idempotent.
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
     * Makes pending writes searchable. Safe to call repeatedly; it is a no-op when nothing changed.
     *
     * <p>This replaces the previous pattern of calling {@link #close()} before searching, which
     * forced the index to be discarded and rebuilt for every query.
     *
     * <p>When a writer exists the reader is opened from it (near real time) rather than from the
     * directory. That deliberately avoids {@code IndexWriter#commit}: the directory is in memory,
     * so a commit point buys no durability and only costs work on every write batch.
     */
    public void refresh() throws IOException {
        if (writeStats) {
            if (readStats && nearRealTimeReader) {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer, true);
                if (newReader != null) {
                    DirectoryReader old = reader;
                    reader = newReader;
                    searcher = new IndexSearcher(newReader);
                    old.close();
                }
            } else {
                final DirectoryReader old = readStats ? reader : null;
                DirectoryReader newReader = DirectoryReader.open(writer);
                reader = newReader;
                searcher = new IndexSearcher(newReader);
                readStats = true;
                nearRealTimeReader = true;
                if (old != null) {
                    old.close();
                }
            }
            pendingWrite = false;
            return;
        }
        if (!readStats) {
            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);
            readStats = true;
            nearRealTimeReader = false;
            return;
        }
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        if (newReader != null) {
            DirectoryReader old = reader;
            reader = newReader;
            searcher = new IndexSearcher(newReader);
            old.close();
        }
    }

    /**
     * Number of live documents currently visible to readers. Reflects the state as of the last
     * {@link #refresh()}.
     */
    public int numDocs() throws IOException {
        ensureSearcher();
        return reader.numDocs();
    }

    public TopDocs searchDoc(String field, String content) throws ParseException, IOException {
        ensureSearcher();
        IndexSearcher current = searcher;
        QueryParser parser = new QueryParser(field, analyzer);
        return current.search(parser.parse(content), Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN);
    }

    public Document getDoc(int docId) {
        try {
            ensureSearcher();
            return searcher.doc(docId);
        } catch (Throwable e) {
            return null;
        }
    }

    /**
     * Opens a reader if one is missing or stale. A no-op once a batch of writes has been followed by
     * {@link #refresh()}, which is what keeps concurrent searches from mutating the store.
     */
    private void ensureSearcher() throws IOException {
        if (!readStats || pendingWrite) {
            refresh();
        }
    }

    /**
     * Opens the writer on first use. A fresh {@link IndexWriterConfig} is built every time, because
     * Lucene rejects reusing a config that has already been handed to a writer.
     */
    public void initWriter() throws IOException {
        if (!writeStats) {
            writer = new IndexWriter(directory, new IndexWriterConfig(analyzer));
            writeStats = true;
        }
    }

    public void close() throws IOException {
        if (writeStats) {
            writer.close();
            writer = null;
            writeStats = false;
            pendingWrite = false;
        }
        if (readStats) {
            reader.close();
            reader = null;
            readStats = false;
            nearRealTimeReader = false;
            searcher = null;
        }
    }

    public Directory getDirectory() {
        return directory;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }
}
