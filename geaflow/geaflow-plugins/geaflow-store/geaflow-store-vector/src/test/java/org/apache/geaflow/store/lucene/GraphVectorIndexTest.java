package org.apache.geaflow.store.lucene;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphVectorIndexTest {

    private GraphVectorIndex<String> stringIndex;
    private GraphVectorIndex<Long> longIndex;
    private GraphVectorIndex<Integer> intIndex;
    private GraphVectorIndex<Float> floatIndex;

    @BeforeMethod
    public void setUp() {
        stringIndex = new GraphVectorIndex<>(String.class);
        longIndex = new GraphVectorIndex<>(Long.class);
        intIndex = new GraphVectorIndex<>(Integer.class);
        floatIndex = new GraphVectorIndex<>(Float.class);
    }

    @AfterMethod
    public void tearDown() {
        stringIndex.close();
        longIndex.close();
        intIndex.close();
        floatIndex.close();
    }

    @Test
    public void testAddAndSearchStringKey() {
        String key = "vertex_1";
        String fieldName = "embedding";
        float[] vector = {0.1f, 0.2f, 0.3f, 0.4f};

        // 添加向量索引
        stringIndex.addVectorIndex(true, key, fieldName, vector);

        // 搜索向量索引
        String result = stringIndex.searchVectorIndex(true, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testAddAndSearchLongKey() {
        Long key = 12345L;
        String fieldName = "embedding";
        float[] vector = {0.5f, 0.6f, 0.7f, 0.8f};

        // 添加向量索引
        longIndex.addVectorIndex(false, key, fieldName, vector);

        // 搜索向量索引
        Long result = longIndex.searchVectorIndex(false, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testAddAndSearchIntegerKey() {
        Integer key = 999;
        String fieldName = "features";
        float[] vector = {0.9f, 0.8f, 0.7f, 0.6f};

        // 添加向量索引
        intIndex.addVectorIndex(true, key, fieldName, vector);

        // 搜索向量索引
        Integer result = intIndex.searchVectorIndex(true, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testAddAndSearchFloatKey() {
        Float key = 3.14f;
        String fieldName = "weights";
        float[] vector = {0.2f, 0.4f, 0.6f, 0.8f};

        // 添加向量索引
        floatIndex.addVectorIndex(false, key, fieldName, vector);

        // 搜索向量索引
        Float result = floatIndex.searchVectorIndex(false, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testVectorSimilaritySearch() {
        String fieldName = "similarity_test";

        // 添加几个具有不同相似度的向量
        stringIndex.addVectorIndex(true, "doc1", fieldName, new float[]{1.0f, 0.0f, 0.0f, 0.0f});
        stringIndex.addVectorIndex(true, "doc2", fieldName, new float[]{0.8f, 0.2f, 0.0f, 0.0f});
        stringIndex.addVectorIndex(true, "doc3", fieldName, new float[]{0.0f, 0.0f, 1.0f, 0.0f});

        // 使用与doc1完全相同的向量进行查询
        float[] queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
        String result = stringIndex.searchVectorIndex(true, fieldName, queryVector, 1);

        // 应该返回最相似的文档
        assertEquals(result, "doc1");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedKeyType() {
        // 使用不支持的键类型
        Double key = 1.23;
        GraphVectorIndex<Double> doubleIndex = new GraphVectorIndex<>(Double.class);

        try {
            doubleIndex.addVectorIndex(true, key, "test_field", new float[]{0.1f, 0.2f});
        } finally {
            doubleIndex.close();
        }
    }

    @Test
    public void testMultipleVectorsSameKeyDifferentFields() {
        String key = "multi_field_vertex";
        float[] vector1 = {0.1f, 0.2f, 0.3f, 0.4f};
        float[] vector2 = {0.5f, 0.6f, 0.7f, 0.8f};

        // 为同一个key添加不同字段的向量
        stringIndex.addVectorIndex(true, key, "field1", vector1);
        stringIndex.addVectorIndex(true, key, "field2", vector2);

        // 分别搜索不同字段
        String result1 = stringIndex.searchVectorIndex(true, "field1", vector1, 1);
        String result2 = stringIndex.searchVectorIndex(true, "field2", vector2, 1);

        assertEquals(result1, key);
        assertEquals(result2, key);
    }
}
