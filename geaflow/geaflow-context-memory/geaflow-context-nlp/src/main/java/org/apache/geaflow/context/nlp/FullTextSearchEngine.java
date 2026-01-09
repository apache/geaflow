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

package org.apache.geaflow.context.nlp;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Full-text search implementation using Apache Lucene.
 * Provides keyword-based and phrase search capabilities for Phase 2.
 */
public class FullTextSearchEngine {

    private static final Logger logger = LoggerFactory.getLogger(
        FullTextSearchEngine.class);

    private final String indexPath;
    private final StandardAnalyzer analyzer;
    private IndexWriter indexWriter;
    private IndexSearcher indexSearcher;
    private DirectoryReader reader;

    /**
     * Constructor with index path.
     *
     * @param indexPath Path to store Lucene index
     * @throws IOException if initialization fails
     */
    public FullTextSearchEngine(String indexPath) throws IOException {
        this.indexPath = indexPath;
        this.analyzer = new StandardAnalyzer();
        initialize();
    }

    /**
     * Initialize the search engine.
     *
     * @throws IOException if initialization fails
     */
    private void initialize() throws IOException {
        try {
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            this.indexWriter = new IndexWriter(
                FSDirectory.open(Paths.get(indexPath)), config);
            refreshSearcher();
            logger.info("FullTextSearchEngine initialized at: {}", indexPath);
        } catch (IOException e) {
            logger.error("Error initializing FullTextSearchEngine", e);
            throw e;
        }
    }

    /**
     * Index a document for full-text search.
     *
     * @param docId Document ID
     * @param content Document content to index
     * @param metadata Additional metadata for filtering
     * @throws IOException if indexing fails
     */
    public void indexDocument(String docId, String content,
        String metadata) throws IOException {
        try {
            Document doc = new Document();
            doc.add(new StringField("id", docId, Field.Store.YES));
            doc.add(new TextField("content", content, Field.Store.YES));
            if (metadata != null) {
                doc.add(new TextField("metadata", metadata, Field.Store.YES));
            }
            indexWriter.addDocument(doc);
            indexWriter.commit();
            refreshSearcher();
        } catch (IOException e) {
            logger.error("Error indexing document: {}", docId, e);
            throw e;
        }
    }

    /**
     * Search for documents using keyword query.
     *
     * @param queryText Query string (supports boolean operators like AND, OR, NOT)
     * @param topK Maximum number of results
     * @return List of search results with IDs and scores
     * @throws IOException if search fails
     */
    public List<SearchResult> search(String queryText, int topK)
        throws IOException {
        List<SearchResult> results = new ArrayList<>();
        try {
            QueryParser parser = new QueryParser("content", analyzer);
            Query query = parser.parse(queryText);
            
            TopDocs topDocs = indexSearcher.search(query, topK);
            
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = indexSearcher.doc(scoreDoc.doc);
                results.add(new SearchResult(
                    doc.get("id"),
                    scoreDoc.score
                ));
            }
            
            logger.debug("Search found {} results for query: {}", 
                results.size(), queryText);
            return results;
        } catch (ParseException e) {
            logger.error("Error parsing search query: {}", queryText, e);
            throw new IOException("Invalid search query", e);
        }
    }

    /**
     * Refresh the searcher to pick up recent changes.
     *
     * @throws IOException if refresh fails
     */
    private void refreshSearcher() throws IOException {
        try {
            if (reader == null) {
                reader = DirectoryReader.open(indexWriter.getDirectory());
            } else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                }
            }
            this.indexSearcher = new IndexSearcher(reader);
        } catch (IOException e) {
            logger.error("Error refreshing searcher", e);
            throw e;
        }
    }

    /**
     * Close and cleanup resources.
     *
     * @throws IOException if close fails
     */
    public void close() throws IOException {
        try {
            if (indexWriter != null) {
                indexWriter.close();
            }
            if (reader != null) {
                reader.close();
            }
            if (analyzer != null) {
                analyzer.close();
            }
            logger.info("FullTextSearchEngine closed");
        } catch (IOException e) {
            logger.error("Error closing FullTextSearchEngine", e);
            throw e;
        }
    }

    /**
     * Search result container.
     */
    public static class SearchResult {

        private final String docId;
        private final float score;

        /**
         * Constructor.
         *
         * @param docId Document ID
         * @param score Relevance score
         */
        public SearchResult(String docId, float score) {
            this.docId = docId;
            this.score = score;
        }

        public String getDocId() {
            return docId;
        }

        public float getScore() {
            return score;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                .append("SearchResult{")
                .append("docId='")
                .append(docId)
                .append("', score=")
                .append(score)
                .append("}")
                .toString();
        }
    }
}
