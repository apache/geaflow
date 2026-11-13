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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

public class SearchStore {

    private final Directory directory = new ByteBuffersDirectory();
    private final Analyzer analyzer = new StandardAnalyzer();
    private final IndexWriterConfig config = new IndexWriterConfig(analyzer);
    private IndexWriter writer;
    private boolean writeStats = false;
    private IndexReader reader;
    private IndexSearcher searcher;
    private boolean readStats = false;
    private boolean closed;

    public SearchStore() {
    }

    public synchronized void addDoc(Map<String, String> kv) throws IOException {
        ensureOpen();
        initWriter();
        Document doc = new Document();
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            doc.add(new TextField(entry.getKey(), entry.getValue(), Field.Store.YES));
        }
        writer.addDocument(doc);
    }

    public TopDocs searchDoc(String field, String content) throws ParseException, IOException {
        return searchDoc(field, content, Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN);
    }

    public synchronized TopDocs searchDoc(String field, String content, int topN)
        throws ParseException, IOException {
        ensureOpen();
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be positive");
        }
        if (!readStats) {
            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);
            readStats = true;
        }
        QueryParser parser = new QueryParser(field, analyzer);
        return searcher.search(parser.parse(content), topN);
    }

    public synchronized Document getDoc(int docId) {
        ensureOpen();
        try {
            if (!readStats) {
                reader = DirectoryReader.open(directory);
                searcher = new IndexSearcher(reader);
                readStats = true;
            }
            return searcher.doc(docId);
        } catch (Throwable e) {
            return null;
        }

    }

    public synchronized void initWriter() throws IOException {
        ensureOpen();
        if (!writeStats) {
            writer = new IndexWriter(directory, config);
            writeStats = true;
        }
    }

    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        if (writeStats) {
            try {
                writer.close();
            } catch (IOException error) {
                failure = error;
            } finally {
                writer = null;
                writeStats = false;
            }
        }
        if (readStats) {
            try {
                reader.close();
            } catch (IOException error) {
                failure = append(failure, error);
            } finally {
                reader = null;
                searcher = null;
                readStats = false;
            }
        }
        try {
            directory.close();
        } catch (IOException error) {
            failure = append(failure, error);
        }
        try {
            analyzer.close();
        } catch (RuntimeException error) {
            IOException wrapped = new IOException("failed to close analyzer", error);
            failure = append(failure, wrapped);
        }
        if (failure != null) {
            throw failure;
        }
    }

    public synchronized void finishWriting() throws IOException {
        if (writeStats) {
            try {
                writer.close();
            } finally {
                writer = null;
                writeStats = false;
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("search store is closed");
        }
    }

    private static IOException append(IOException current, IOException next) {
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
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
