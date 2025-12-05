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

import java.io.IOException;
import java.util.Map;

public class SearchStore {

    private final Directory directory = new ByteBuffersDirectory();
    private final Analyzer analyzer = new StandardAnalyzer();
    private final IndexWriterConfig config = new IndexWriterConfig(analyzer);
    private IndexWriter writer;
    private boolean writeStats = false;
    private IndexReader reader;
    private IndexSearcher searcher;
    private boolean readStats = false;

    public SearchStore() {
    }

    public void addDoc(Map<String, String> kv) throws IOException {
        initWriter();
        Document doc = new Document();
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            doc.add(new TextField(entry.getKey(), entry.getValue(), Field.Store.YES));
        }
        writer.addDocument(doc);
    }

    public TopDocs searchDoc(String field, String content) throws ParseException, IOException {
        if (!readStats) {
            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);
            readStats = true;
        }
        QueryParser parser = new QueryParser(field, analyzer);
        return searcher.search(parser.parse(content), 10);
    }

    public Document getDoc(int docId) {
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
        }
        if (readStats) {
            reader.close();
            readStats = false;
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