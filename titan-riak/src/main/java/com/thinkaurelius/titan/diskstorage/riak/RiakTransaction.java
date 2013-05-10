package com.thinkaurelius.titan.diskstorage.riak;

import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

public class RiakTransaction extends AbstractStoreTransaction {
    public RiakTransaction(ConsistencyLevel level) {
        super(ConsistencyLevel.KEY_CONSISTENT);
    }
}
