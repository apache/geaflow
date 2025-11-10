package org.apache.geaflow.dsl.connector.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.CloseableIterator;

public class IteratorWrapper implements CloseableIterator<Object> {

    private final CloseableIterator<InternalRow> iterator;
    private final PaimonRecordDeserializer deserializer;

    public IteratorWrapper(CloseableIterator<InternalRow> iterator, PaimonRecordDeserializer deserializer) {
        this.iterator = iterator;
        this.deserializer = deserializer;
    }

    @Override
    public void close() throws Exception {
        iterator.close();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Object next() {
        return deserializer.deserialize(iterator.next());
    }
}
