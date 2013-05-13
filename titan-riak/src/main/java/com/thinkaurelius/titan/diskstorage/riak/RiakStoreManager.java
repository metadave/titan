package com.thinkaurelius.titan.diskstorage.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class RiakStoreManager  extends DistributedStoreManager implements KeyValueStoreManager {

    public static final String KEYSPACE_DEFAULT = "titan";
    public static final String KEYSPACE_KEY = "keyspace";

    private StoreFeatures features = null;
    private static final int PORT_DEFAULT = 10017;
    IRiakClient riakClient;

    private Map<String, RiakKeyValueStore> stores = new HashMap<String, RiakKeyValueStore>();

    public RiakStoreManager(org.apache.commons.configuration.Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);
    }

    @Override
    public KeyValueStore openDatabase(String name) throws StorageException {
        try {
            if(stores.containsKey(name)) {
                return stores.get(name);
            } else {
                System.out.println("Opening Riak [" + name + "]");
                riakClient = RiakFactory.pbcClient("127.0.0.1",10017);
                RiakKeyValueStore store = new RiakKeyValueStore(riakClient, name);
                stores.put(name, store);
                return store;
            }
        } catch (RiakException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        return new RiakTransaction(level);
    }

    @Override
    public void close() throws StorageException {
        System.out.println("CLOSE");
    }

    @Override
    public void clearStorage() throws StorageException {
        System.out.println("CLEAR STORAGE");
    }

    @Override
    public StoreFeatures getFeatures() {
        if (features == null) {
            System.out.println("New features");
            features = new StoreFeatures();
            features.supportsScan = true;
            features.supportsBatchMutation = false;
            features.supportsTransactions = false;
            // ??
            features.supportsConsistentKeyOperations = true;
            features.supportsLocking = false;
            features.isDistributed = true;
            features.isKeyOrdered = false;
            features.hasLocalKeyPartition = false;
        }
        return features;
    }

    public static final String TITAN_BACKEND_VERSION = "titan-version";

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        if(key.equals(TITAN_BACKEND_VERSION)) {
            return "0.4.0-SNAPSHOT";
        }
        System.err.println("Getting config property " + key);
        return "";
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        System.err.println("Setting config property " + key + ":" + value);
    }
}
