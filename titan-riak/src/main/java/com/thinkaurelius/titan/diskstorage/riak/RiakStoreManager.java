package com.thinkaurelius.titan.diskstorage.riak;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Map;

public class RiakStoreManager  extends DistributedStoreManager implements KeyColumnValueStoreManager {

    //private final StoreFeatures features;
    //private final FileStorageConfiguration storageConfig;

    private static final int PORT_DEFAULT = 10018;

    public RiakStoreManager(Configuration config) {
        super(config, PORT_DEFAULT);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws StorageException {
        return null;
    }

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {

    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException {
        return null;
    }

    @Override
    public void close() throws StorageException {

    }

    @Override
    public void clearStorage() throws StorageException {

    }

    @Override
    public StoreFeatures getFeatures() {
        return null;
    }

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        return null;
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {

    }
}
