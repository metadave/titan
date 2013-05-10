package com.thinkaurelius.titan.diskstorage.riak;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.nio.ByteBuffer;
import java.util.List;


public class RiakKeyColumnValueStore implements KeyColumnValueStore {
    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        return false;
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return null;
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return null;
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return false;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {

    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {

    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return null;
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        return new ByteBuffer[0];
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void close() throws StorageException {

    }
}
