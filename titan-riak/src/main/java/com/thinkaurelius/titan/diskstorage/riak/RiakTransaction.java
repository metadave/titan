package com.thinkaurelius.titan.diskstorage.riak;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

public class RiakTransaction extends AbstractStoreTransaction {

    public RiakTransaction(ConsistencyLevel level) {
        super(level);
    }

    @Override
    public synchronized void rollback() throws StorageException {
        System.out.println("CANT ROLLBACK");
    }
}
