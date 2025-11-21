package org.apache.geaflow.store.lucene;

import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.IntField;
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

    @Override
    public void addVectorIndex(boolean isVertex, K key, String fieldName, float[] vector) {
        try {
            // 创建文档
            Document doc = new Document();

            // 根据K的类型选择不同的Field
            if (key instanceof Float) {
                doc.add(new FloatField(KEY_FIELD_NAME, (float) key, Field.Store.YES));
            } else if (key instanceof Long) {
                doc.add(new LongField(KEY_FIELD_NAME, (long) key, Field.Store.YES));
            } else if (key instanceof Integer) {
                doc.add(new IntField(KEY_FIELD_NAME, (int) key, Field.Store.YES));
            } else if (key instanceof String) {
                doc.add(new TextField(KEY_FIELD_NAME, (String) key, Field.Store.YES));
            } else {
                throw new IllegalArgumentException("Unsupported key type: " + key.getClass().getName());
            }

            // 添加向量字段
            doc.add(new KnnVectorField(fieldName, vector));

            // 将文档添加到索引中
            writer.addDocument(doc);

            // 提交写入操作
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException("Failed to add vector index", e);
        }
    }

    @Override
    public K searchVectorIndex(boolean isVertex, String fieldName, float[] vector, int topK) {
        try {
            // 打开索引读取器
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            // 创建KNN向量查询
            KnnVectorQuery knnQuery = new KnnVectorQuery(fieldName, vector, topK);

            // 执行搜索
            TopDocs topDocs = searcher.search(knnQuery, topK);

            Document firstDoc = searcher.doc(topDocs.scoreDocs[0].doc);
            String value = firstDoc.get(KEY_FIELD_NAME);

            K result;
            if (keyClass == String.class) {
                result = (K) value;
            } else if (keyClass == Long.class) {
                result = (K) Long.valueOf(value);
            } else if (keyClass == Integer.class) {
                result = (K) Integer.valueOf(value);
            } else if (keyClass == Float.class) {
                result = (K) Float.valueOf(value);
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
     * 关闭索引写入器和目录资源
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
