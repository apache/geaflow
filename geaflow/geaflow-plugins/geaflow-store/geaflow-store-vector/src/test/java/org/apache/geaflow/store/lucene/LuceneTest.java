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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.testng.annotations.Test;

public class LuceneTest {

    @Test
    public void test() throws IOException {
        // Use ByteBuffersDirectory instead of RAMDirectory
        Directory directory = new ByteBuffersDirectory();
        StandardAnalyzer analyzer = new StandardAnalyzer();

        // Create index writer configuration
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        // Add documents (including vector fields)
        addDocuments(writer);

        // Commit and close writer
        writer.commit();
        writer.close();

        // Search example
        searchDocuments(directory, analyzer);

        // Close directory
        directory.close();
    }


    private static void addDocuments(IndexWriter writer) throws IOException {
        // Document 1
        Document doc1 = new Document();
        doc1.add(new TextField("title", "机器学习基础", Field.Store.YES));
        doc1.add(new TextField("content", "机器学习是人工智能的重要分支", Field.Store.YES));
        // Add vector field
        float[] vector1 = {0.1f, 0.2f, 0.3f, 0.4f};
        doc1.add(new KnnVectorField("vector_field", vector1));
        writer.addDocument(doc1);

        // Document 2
        Document doc2 = new Document();
        doc2.add(new TextField("title", "深度学习入门", Field.Store.YES));
        doc2.add(new TextField("content", "深度学习使用神经网络进行学习", Field.Store.YES));
        float[] vector2 = {0.2f, 0.3f, 0.4f, 0.5f};
        doc2.add(new KnnVectorField("vector_field", vector2));
        writer.addDocument(doc2);

        // Document 3
        Document doc3 = new Document();
        doc3.add(new TextField("title", "自然语言处理", Field.Store.YES));
        doc3.add(new TextField("content", "NLP是处理文本的技术", Field.Store.YES));
        float[] vector3 = {0.15f, 0.25f, 0.35f, 0.45f};
        doc3.add(new KnnVectorField("vector_field", vector3));
        writer.addDocument(doc3);
    }

    private static void searchDocuments(Directory directory, StandardAnalyzer analyzer) throws IOException {
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Create query vector
        float[] queryVector = {0.12f, 0.22f, 0.32f, 0.42f};

        // Execute KNN search
        KnnVectorQuery knnQuery = new KnnVectorQuery("vector_field", queryVector, 3);

        // Execute search
        TopDocs topDocs = searcher.search(knnQuery, 10);

        // Verify search results
        assertEquals(topDocs.scoreDocs.length, 3, "Should return 3 results");

        // Verify results are sorted by relevance (scores from high to low)
        for (int i = 0; i < topDocs.scoreDocs.length - 1; i++) {
            assertTrue(topDocs.scoreDocs[i].score >= topDocs.scoreDocs[i + 1].score,
                "Results should be sorted by score from high to low");
        }

        // Verify each result has expected fields
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document doc = searcher.doc(scoreDoc.doc);
            assertNotNull(doc.get("title"), "Title field should not be null");
            assertNotNull(doc.get("content"), "Content field should not be null");
        }

        reader.close();
    }

    // Add more test cases to test Lucene vector functionality

    @Test
    public void testVectorSearchAccuracy() throws IOException {
        Directory directory = new ByteBuffersDirectory();
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        // Create test data to ensure vector similarity has obvious differences
        Document doc1 = new Document();
        doc1.add(new TextField("id", "1", Field.Store.YES));
        // Exactly the same vector, should get the highest score
        float[] vector1 = {1.0f, 0.0f, 0.0f, 0.0f};
        doc1.add(new KnnVectorField("vector_field", vector1));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new TextField("id", "2", Field.Store.YES));
        // Partially similar vector
        float[] vector2 = {0.8f, 0.2f, 0.0f, 0.0f};
        doc2.add(new KnnVectorField("vector_field", vector2));
        writer.addDocument(doc2);

        Document doc3 = new Document();
        doc3.add(new TextField("id", "3", Field.Store.YES));
        // Completely different vector, should get the lowest score
        float[] vector3 = {0.0f, 0.0f, 1.0f, 0.0f};
        doc3.add(new KnnVectorField("vector_field", vector3));
        writer.addDocument(doc3);

        writer.commit();
        writer.close();

        // Execute search
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Query vector is exactly the same as the first document
        float[] queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
        KnnVectorQuery knnQuery = new KnnVectorQuery("vector_field", queryVector, 3);
        TopDocs topDocs = searcher.search(knnQuery, 10);

        // Verify result count
        assertEquals(topDocs.scoreDocs.length, 3);

        // Verify the first result should be the document with id 1 (highest score)
        Document firstDoc = searcher.doc(topDocs.scoreDocs[0].doc);
        assertEquals(firstDoc.get("id"), "1");

        // Verify score order (first score should be highest)
        assertTrue(topDocs.scoreDocs[0].score >= topDocs.scoreDocs[1].score);
        assertTrue(topDocs.scoreDocs[1].score >= topDocs.scoreDocs[2].score);

        reader.close();
        directory.close();
    }

    @Test
    public void testVectorFieldProperties() throws IOException {
        // Test basic properties of vector fields
        float[] vector = {0.1f, 0.2f, 0.3f, 0.4f};
        KnnVectorField vectorField = new KnnVectorField("test_vector", vector);

        // Verify field name
        assertEquals(vectorField.name(), "test_vector");

        // Verify field value
        assertEquals(vectorField.vectorValue(), vector);
    }

    @Test
    public void testMultipleVectorFields() throws IOException {
        Directory directory = new ByteBuffersDirectory();
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        // Create document with multiple vector fields
        Document doc = new Document();
        doc.add(new TextField("title", "多向量测试", Field.Store.YES));

        // Add first vector field
        float[] vector1 = {0.1f, 0.2f, 0.3f, 0.4f};
        doc.add(new KnnVectorField("vector_field_1", vector1));

        // Add second vector field
        float[] vector2 = {0.5f, 0.6f, 0.7f, 0.8f};
        doc.add(new KnnVectorField("vector_field_2", vector2));

        writer.addDocument(doc);
        writer.commit();
        writer.close();

        // Verify document in index
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        assertEquals(reader.numDocs(), 1);

        // Test search on first vector field
        KnnVectorQuery query1 = new KnnVectorQuery("vector_field_1", vector1, 1);
        TopDocs results1 = searcher.search(query1, 10);
        assertEquals(results1.scoreDocs.length, 1);

        // Test search on second vector field
        KnnVectorQuery query2 = new KnnVectorQuery("vector_field_2", vector2, 1);
        TopDocs results2 = searcher.search(query2, 10);
        assertEquals(results2.scoreDocs.length, 1);

        reader.close();
        directory.close();
    }

}
