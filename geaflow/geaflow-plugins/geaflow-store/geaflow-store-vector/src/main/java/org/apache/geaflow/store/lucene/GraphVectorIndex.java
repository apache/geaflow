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

package org.apache.geaflow.store.lucene;

import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

/**
 * Graph vector index implementation using Apache Lucene.
 *
 * <p><strong>Thread Safety Note:</strong> This class is <strong>not thread-safe</strong>.
 * All public methods in this class are <strong>not designed for concurrent access</strong>.
 * External synchronization is required if instances of this class may be accessed
 * from multiple threads.</p>
 *
 * <p>Specifically:</p>
 * <ul>
 *   <li>{@link #addVectorIndex} uses a non-thread-safe {@link IndexWriter}</li>
 *   <li>{@link #searchVectorIndex} may see inconsistent data during concurrent writes</li>
 *   <li>The underlying {@link ByteBuffersDirectory} is not designed for multi-threaded access</li>
 * </ul>
 *
 * @param <K> the type of keys used to identify vectors
 */
public class GraphVectorIndex<K> implements IVectorIndex<K> {

    private final Directory directory;
    private final IndexWriter writer;
    private final Class<K> keyClass;

    private static final String KEY_FIELD_NAME = "key_field";

    public GraphVectorIndex(Class<K> keyClas) {
        try {
            this.directory = new ByteBuffersDirectory();
            StandardAnalyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            this.writer = new IndexWriter(directory, config);
            this.keyClass = keyClas;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize GraphVectorIndex", e);
        }
    }

    /**
     * Adds a vector to the index with the given key.
     *
     * <p><strong>Note:</strong> This method is <strong>not thread-safe</strong>.
     * The underlying {@link IndexWriter} is not designed for concurrent access.
     * External synchronization is required if this method may be called from
     * multiple threads.</p>
     *
     * @param isVertex whether this is a vertex (unused in current implementation)
     * @param key the key associated with the vector
     * @param fieldName the name of the vector field in the index
     * @param vector the vector data to be indexed
     * @throws RuntimeException if an I/O error occurs during indexing
     */
    @Override
    public void addVectorIndex(boolean isVertex, K key, String fieldName, float[] vector) {
        try {
            // Create document
            Document doc = new Document();

            // Select different Field based on the type of K
            if (key instanceof Float) {
                doc.add(new StoredField(KEY_FIELD_NAME, (float) key));
            } else if (key instanceof Long) {
                doc.add(new StoredField(KEY_FIELD_NAME, (long) key));
            } else if (key instanceof Integer) {
                doc.add(new StoredField(KEY_FIELD_NAME, (int) key));
            } else if (key instanceof String) {
                doc.add(new TextField(KEY_FIELD_NAME, (String) key, Field.Store.YES));
            } else {
                throw new IllegalArgumentException("Unsupported key type: " + key.getClass().getName());
            }

            // Add vector field
            doc.add(new KnnVectorField(fieldName, vector));

            // Add document to index
            writer.addDocument(doc);

            // Commit write operation
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException("Failed to add vector index", e);
        }
    }

    /**
     * Searches for the closest vector in the index.
     *
     * <p><strong>Note:</strong> This method is <strong>not thread-safe</strong>.
     * The underlying index access is not synchronized with write operations.
     * External synchronization is required if this method may be called concurrently
     * with {@link #addVectorIndex}.</p>
     *
     * @param isVertex whether this is a vertex (unused in current implementation)
     * @param fieldName the name of the vector field to search
     * @param vector the query vector
     * @param topK the number of top results to return
     * @return the key associated with the closest vector
     * @throws RuntimeException if an I/O error occurs during search
     */
    @Override
    public K searchVectorIndex(boolean isVertex, String fieldName, float[] vector, int topK) {
        try {
            // Open index reader
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            // Create KNN vector query
            KnnVectorQuery knnQuery = new KnnVectorQuery(fieldName, vector, topK);

            // Execute search
            TopDocs topDocs = searcher.search(knnQuery, topK);

            Document firstDoc = searcher.doc(topDocs.scoreDocs[0].doc);

            K result;
            if (keyClass == String.class) {
                String value = firstDoc.get(KEY_FIELD_NAME);
                result = (K) value;
            } else if (keyClass == Long.class) {
                Number value = firstDoc.getField(KEY_FIELD_NAME).numericValue();
                result = (K) Long.valueOf(value.longValue());
            } else if (keyClass == Integer.class) {
                Number value = firstDoc.getField(KEY_FIELD_NAME).numericValue();
                result = (K) Integer.valueOf(value.intValue());
            } else if (keyClass == Float.class) {
                Number value = firstDoc.getField(KEY_FIELD_NAME).numericValue();
                result = (K) Float.valueOf(value.floatValue());
            } else {
                throw new IllegalArgumentException("Unsupported key type: " + keyClass.getName());
            }

            reader.close();

            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to search vector index", e);
        }
    }

    /**
     * Close index writer and directory resources.
     */
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
            if (directory != null) {
                directory.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close resources", e);
        }
    }
}
